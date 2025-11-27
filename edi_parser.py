"""Utilities for reconstructing patient and encounter data from EDI claim files.

The script understands the fixed-width K020.* health-insurance claim layout that
appears under folders such as ``data/test_source/mi/YYYYMM`` or
``data/test_source/ta/YYYYMM``.  It reads the K020.1~K020.4 (health insurance) and
C110.1~C110.4 (auto insurance) files, extracts the patient master information,
diagnosis rows, billing items, and merges line-level specific details directly
into each encounter item, ultimately emitting CSV snapshots that mirror the
``Patient`` and ``Encounter`` oriented data model described in the root
instructions text file.

Usage example::

    python edi_parser.py --source data/test_source --output-dir parsed_output

The command above will look for every directory beneath ``data/test_source`` that
contains either a ``K020.1`` or ``C110.1`` file, parse the companion ``*.2``
through ``*.4`` files when present, and then materialize four CSV files inside
the ``parsed_output`` directory:

* patients.csv - One row per encounter specific patient slice.
* encounters.csv - Encounter level metadata (date, department, license info).
* encounter_dx.csv - Diagnosis rows linked to each encounter.
* encounter_items.csv - Itemized billing / prescription content.
* insurances.csv - Insurance / payer level metadata per encounter (K020.1).
* invoices.csv - Invoice-level billing aggregates per encounter.

Both Korean and English characters inside the source files are preserved.  Field
positions follow the specification inside ``지시사항.txt`` (with alternate
offsets for the auto-insurance C110 layout).  When the claim text uses Korean,
remember that the underlying files are encoded with double-byte characters; the
parser therefore slices byte positions rather than Python character offsets.
"""

from __future__ import annotations

import argparse
import csv
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ENCODING_DEFAULT = "cp949"
OUTPUT_ENCODING_DEFAULT = "cp949"
DX_TYPE_LABELS = {"1": "primary", "2": "secondary"}
LICENSE_TYPE_LABELS = {"1": "doctor", "2": "dentist", "3": "oriental_doctor"}

AUTO_INSURER_COMPANIES = {
    "01": "메리츠화재",
    "02": "한화손보",
    "03": "롯데손보",
    "04": "MG손보",
    "05": "흥국화재",
    "08": "삼성화재",
    "09": "현대해상",
    "10": "KB손보",
    "11": "DB손보",
    "17": "AIG손보",
    "21": "전국택시공제조합",
    "22": "전국버스공제조합",
    "23": "전국화물자동차공제조합",
    "24": "전국개인택시공제조합",
    "25": "전국전세버스공제조합",
    "28": "배달서비스공제조합",
    "30": "전국렌터카공제조합",
    "41": "AXA손보",
    "42": "하나손보",
    "43": "신한EZ손보",
    "45": "현대해상하이카다이렉트",
    "69": "캐롯손보",
    "93": "자동차손해배상진흥원",
    "98": "에이스아메리칸화재",
    "99": "미군공제(USAA)",
}


@dataclass(frozen=True)
class ClaimFileLayout:
    patient_file: str
    dx_file: str
    item_file: str
    detail_file: str
    patient_name_start: int
    patient_name_length: int
    patient_identity_start: int
    patient_identity_length: int


MI_LAYOUT = ClaimFileLayout(
    patient_file="K020.1",
    dx_file="K020.2",
    item_file="K020.3",
    detail_file="K020.4",
    patient_name_start=105,
    patient_name_length=20,
    patient_identity_start=125,
    patient_identity_length=13,
)

AUTO_LAYOUT = ClaimFileLayout(
    patient_file="C110.1",
    dx_file="C110.2",
    item_file="C110.3",
    detail_file="C110.4",
    patient_name_start=98,
    patient_name_length=20,
    patient_identity_start=118,
    patient_identity_length=13,
)

SUPPORTED_LAYOUTS = [MI_LAYOUT, AUTO_LAYOUT]


def _parse_gender(identity_suffix: str) -> Optional[str]:
    if not identity_suffix:
        return None
    code = identity_suffix[0]
    if code in {"1", "3", "5", "7", "9"}:
        return "M"
    if code in {"0", "2", "4", "6", "8"}:
        return "F"
    return None


def _parse_date(value: str) -> Optional[date]:
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        logging.warning("Unable to parse encounter date: %s", value)
        return None


def _parse_decimal(value: str, scale: int) -> Optional[Decimal]:
    raw = value.strip()
    if not raw:
        return None
    try:
        quant = Decimal(raw)
    except Exception:  # noqa: BLE001
        logging.warning("Unable to parse decimal value: %s", value)
        return None
    scale_factor = Decimal(10) ** scale
    return (quant / scale_factor).quantize(Decimal(1) / (Decimal(10) ** scale), rounding=ROUND_HALF_UP)


def _format_decimal(value: Optional[Decimal], places: int = 2) -> str:
    if value is None:
        return ""
    quant = Decimal(1) / (Decimal(10) ** places)
    return format(value.quantize(quant), f".{places}f")


def _parse_int(value: str) -> Optional[int]:
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return int(stripped)
    except ValueError:
        logging.warning("Unable to parse integer value: %s", value)
        return None


def _parse_amount(value: str) -> int:
    stripped = value.strip().replace(",", "")
    if not stripped:
        return 0
    try:
        return int(stripped)
    except ValueError:
        logging.warning("Unable to parse amount value: %s", value)
        return 0


def _derive_insurance_nhis_type(medical_code: str, special_code: str) -> str:
    if special_code == "C":
        return "11"
    if special_code in {"E", "F"}:
        return "12"
    mapping = {
        "": "1",
        "1": "7",
        "2": "8",
        "4": "4",
        "6": "9",
        "8": "9",
    }
    return mapping.get(medical_code, medical_code or "1")


@dataclass
class PatientRecord:
    encounter_no: str
    claim_no: str
    statement_no: str
    patient_name: str
    patient_identity_prefix: str
    patient_identity_suffix: str
    patient_gender: Optional[str]
    extra_fields: Dict[str, str] = field(default_factory=dict)

    def to_row(self) -> Dict[str, str]:
        base = {
            "encounter_no": self.encounter_no,
            "patient_name": self.patient_name,
            "patient_identity_prefix": self.patient_identity_prefix,
            "patient_identity_suffix": self.patient_identity_suffix,
            "patient_gender": self.patient_gender or "",
        }
        if not self.extra_fields:
            return base
        enriched = dict(base)
        enriched.update(self.extra_fields)
        return enriched


@dataclass
class InsuranceRecord:
    encounter_no: str
    insurance_type_code: str = ""
    insurance_nhis_type: str = ""
    insurance_nhis_no: str = ""
    insurance_form_no: str = ""
    insurance_provider_code: str = ""
    insurance_payer_code: str = ""
    insurance_claim_type: str = ""
    insurance_subscriber_name: str = ""
    insurance_medical_aid_code: str = ""
    insurance_special_code: str = ""
    insurance_copay_code: str = ""
    claim_no: str = ""
    statement_no: str = ""
    insurance_ta_reg_no: str = ""
    insurance_ta_ins_no: str = ""
    insurance_ta_company_code: str = ""
    insurance_ta_company_name: str = ""

    def to_row(self) -> Dict[str, str]:
        return {
            "encounter_no": self.encounter_no,
            "insurance_type_code": self.insurance_type_code,
            "insurance_nhis_type": self.insurance_nhis_type,
            "insurance_nhis_no": self.insurance_nhis_no,
            "insurance_form_no": self.insurance_form_no,
            "insurance_provider_code": self.insurance_provider_code,
            "insurance_payer_code": self.insurance_payer_code,
            "insurance_claim_type": self.insurance_claim_type,
            "insurance_subscriber_name": self.insurance_subscriber_name,
            "insurance_medical_aid_code": self.insurance_medical_aid_code,
            "insurance_special_code": self.insurance_special_code,
            "insurance_copay_code": self.insurance_copay_code,
            "claim_no": self.claim_no,
            "statement_no": self.statement_no,
            "insurance_ta_reg_no": self.insurance_ta_reg_no,
            "insurance_ta_ins_no": self.insurance_ta_ins_no,
            "insurance_ta_company_code": self.insurance_ta_company_code,
            "insurance_ta_company_name": self.insurance_ta_company_name,
        }


@dataclass
class InvoiceRecord:
    encounter_no: str
    invoice_sum: int = 0
    invoice_insurance_sum: int = 0
    invoice_insurance_patient_burden: int = 0
    invoice_insurance_nhis_burden: int = 0
    invoice_subsidy: int = 0
    invoice_is_fixed_patient_burden: bool = False
    invoice_patient_max_excess: int = 0
    invoice_disabled_support: int = 0

    def to_row(self) -> Dict[str, str]:
        return {
            "encounter_no": self.encounter_no,
            "invoice_sum": str(self.invoice_sum),
            "invoice_insurance_sum": str(self.invoice_insurance_sum),
            "invoice_insurance_patient_burden": str(self.invoice_insurance_patient_burden),
            "invoice_insurance_nhis_burden": str(self.invoice_insurance_nhis_burden),
            "invoice_subsidy": str(self.invoice_subsidy),
            "invoice_is_fixed_patient_burden": "Y" if self.invoice_is_fixed_patient_burden else "N",
            "invoice_patient_max_excess": str(self.invoice_patient_max_excess),
            "invoice_disabled_support": str(self.invoice_disabled_support),
        }


@dataclass
class EncounterDxRecord:
    encounter_no: str
    dx_type_code: str
    kcd_code: str

    def to_row(self) -> Dict[str, str]:
        return {
            "encounter_no": self.encounter_no,
            "dx_type_code": self.dx_type_code,
            "dx_type_label": DX_TYPE_LABELS.get(self.dx_type_code, ""),
            "kcd_code": self.kcd_code,
        }


@dataclass
class EncounterItemRecord:
    encounter_no: str
    line_no: str
    item_code: str
    daily_amount: Optional[Decimal]
    days: Optional[int]
    detail_texts: List[str] = field(default_factory=list)

    def to_row(self) -> Dict[str, str]:
        return {
            "encounter_no": self.encounter_no,
            "line_no": self.line_no,
            "encounter_item_number": f"{self.encounter_no}{self.line_no}",
            "item_code": self.item_code,
            "daily_amount": _format_decimal(self.daily_amount, 2),
            "days": str(self.days) if self.days is not None else "",
            "detail_text": " | ".join(self.detail_texts),
        }

    def add_detail_text(self, text: str) -> None:
        cleaned = text.strip()
        if cleaned:
            self.detail_texts.append(cleaned)


@dataclass
class EncounterRecord:
    encounter_no: str
    patient: Optional[PatientRecord] = None
    encounter_date: Optional[date] = None
    department_code: str = ""
    license_type_code: str = ""
    license_no: str = ""
    dx_list: List[EncounterDxRecord] = field(default_factory=list)
    items: List[EncounterItemRecord] = field(default_factory=list)
    treatment_days: Optional[int] = None
    inpatient_days: Optional[int] = None
    result_code: str = ""
    is_gongsang: bool = False
    copay_type_code: str = ""
    insurance: Optional[InsuranceRecord] = None
    invoice: Optional[InvoiceRecord] = None

    def to_row(self) -> Dict[str, str]:
        return {
            "encounter_no": self.encounter_no,
            "patient_encounter_no": self.patient.encounter_no if self.patient else "",
            "encounter_date": self.encounter_date.isoformat() if self.encounter_date else "",
            "license_no": self.license_no,
            "has_dx": "Y" if self.dx_list else "N",
            "has_items": "Y" if self.items else "N",
            "treatment_days": str(self.treatment_days) if self.treatment_days is not None else "",
            "inpatient_days": str(self.inpatient_days) if self.inpatient_days is not None else "",
            "result_code": self.result_code,
            "is_gongsang": "Y" if self.is_gongsang else "N",
            "copay_type_code": self.copay_type_code,
        }


class EDIClaimParser:
    def __init__(self, base_path: Path, encoding: str = ENCODING_DEFAULT) -> None:
        self.base_path = base_path
        self.encoding = encoding

    def discover_claim_dirs(self) -> List[Tuple[Path, ClaimFileLayout]]:
        """Return every directory that contains a supported claim file."""
        return self._discover_claim_dirs()

    def parse_with_status(
        self,
    ) -> Tuple[
        Dict[str, EncounterRecord],
        List[Path],
        Dict[Path, str],
        Dict[str, List[str]],
    ]:
        encounters: Dict[str, EncounterRecord] = {}
        claim_dirs = self._discover_claim_dirs()
        if not claim_dirs:
            raise FileNotFoundError(f"Could not find any claim directories under {self.base_path}")
        successes: List[Path] = []
        failures: Dict[Path, str] = {}
        month_map: Dict[str, List[str]] = {"건보": [], "자보": []}
        for claim_dir, layout in claim_dirs:
            try:
                logging.info("Parsing claim folder %s", claim_dir)
                claim_encounters = self._parse_claim_dir(claim_dir, layout)
            except Exception as exc:  # noqa: BLE001
                logging.exception("Failed to parse claim folder %s", claim_dir)
                failures[claim_dir] = str(exc)
                continue
            successes.append(claim_dir)
            month = self._extract_claim_month(claim_dir / layout.patient_file)
            month_key = self._format_month(month) if month else "알수없음"
            bucket = "건보" if layout is MI_LAYOUT else "자보"
            month_map.setdefault(bucket, []).append(month_key)
            for encounter_no, record in claim_encounters.items():
                if encounter_no in encounters:
                    self._merge_records(encounters[encounter_no], record)
                else:
                    encounters[encounter_no] = record
        return encounters, successes, failures, month_map

    def parse(self) -> Dict[str, EncounterRecord]:
        encounters, successes, failures, _ = self.parse_with_status()
        if failures:
            failed_list = ", ".join(str(path) for path in failures.keys())
            raise RuntimeError(f"Failed to parse claim folders: {failed_list}")
        return encounters

    def _extract_claim_month(self, patient_file: Path) -> Optional[str]:
        if not patient_file.exists():
            return None
        try:
            with patient_file.open("rb") as handle:
                line = handle.readline()
        except OSError:
            return None
        if not line:
            return None
        claim_no = self._slice_text(line.rstrip(b"\r\n"), 1, 10)
        if len(claim_no) < 6:
            return None
        return claim_no[:6]

    @staticmethod
    def _format_month(value: Optional[str]) -> str:
        if not value or len(value) != 6 or not value.isdigit():
            return "알수없음"
        year = value[:4]
        month = value[4:6]
        return f"{year}.{month}"

    def _merge_records(self, target: EncounterRecord, source: EncounterRecord) -> None:
        if source.patient and not target.patient:
            target.patient = source.patient
        if source.encounter_date and not target.encounter_date:
            target.encounter_date = source.encounter_date
        if source.department_code and not target.department_code:
            target.department_code = source.department_code
        if source.license_type_code and not target.license_type_code:
            target.license_type_code = source.license_type_code
        if source.license_no and not target.license_no:
            target.license_no = source.license_no
        target.dx_list.extend(source.dx_list)
        target.items.extend(source.items)
        if source.treatment_days and not target.treatment_days:
            target.treatment_days = source.treatment_days
        if source.inpatient_days and not target.inpatient_days:
            target.inpatient_days = source.inpatient_days
        if source.result_code and not target.result_code:
            target.result_code = source.result_code
        if source.copay_type_code and not target.copay_type_code:
            target.copay_type_code = source.copay_type_code
        if source.is_gongsang and not target.is_gongsang:
            target.is_gongsang = source.is_gongsang
        if source.insurance and not target.insurance:
            target.insurance = source.insurance
        if source.invoice and not target.invoice:
            target.invoice = source.invoice

    def _discover_claim_dirs(self) -> List[Tuple[Path, ClaimFileLayout]]:
        base = self.base_path
        candidates: Dict[Path, ClaimFileLayout] = {}
        for layout in SUPPORTED_LAYOUTS:
            direct_file = base / layout.patient_file
            if direct_file.exists():
                candidates[base] = layout
            for path in base.rglob(layout.patient_file):
                candidates.setdefault(path.parent, layout)
        return sorted(candidates.items(), key=lambda item: str(item[0]))

    def _parse_claim_dir(self, claim_dir: Path, layout: ClaimFileLayout) -> Dict[str, EncounterRecord]:
        encounters: Dict[str, EncounterRecord] = {}
        patient_map, insurance_map, invoice_map, encounter_meta = self._parse_patient_file(
            claim_dir / layout.patient_file,
            layout,
        )
        for encounter_no, patient in patient_map.items():
            meta = encounter_meta.get(encounter_no, {})
            encounters[encounter_no] = EncounterRecord(
                encounter_no=encounter_no,
                patient=patient,
                treatment_days=meta.get("treatment_days"),
                inpatient_days=meta.get("inpatient_days"),
                result_code=meta.get("result_code", ""),
                is_gongsang=meta.get("is_gongsang", False),
                copay_type_code=meta.get("copay_type_code", ""),
                insurance=insurance_map.get(encounter_no),
                invoice=invoice_map.get(encounter_no),
            )
        self._attach_dx(encounters, claim_dir / layout.dx_file)
        self._attach_items(encounters, claim_dir / layout.item_file)
        self._attach_details(encounters, claim_dir / layout.detail_file)
        return encounters

    def _attach_dx(self, encounters: Dict[str, EncounterRecord], path: Path) -> None:
        for line in self._iter_lines(path):
            encounter_no = self._slice_text(line, 1, 15)
            if not encounter_no:
                continue
            dx_type_code = self._slice_text(line, 16, 1)
            kcd_code = self._slice_text(line, 17, 6)
            department_code = self._slice_text(line, 23, 2)
            encounter_date_str = self._slice_text(line, 25, 8)
            license_type_code = self._slice_text(line, 33, 1)
            license_no = self._slice_text(line, 34, 10)
            encounter_record = encounters.setdefault(encounter_no, EncounterRecord(encounter_no=encounter_no))
            encounter_date = _parse_date(encounter_date_str)
            if encounter_date and not encounter_record.encounter_date:
                encounter_record.encounter_date = encounter_date
            if department_code and not encounter_record.department_code:
                encounter_record.department_code = department_code
            if license_type_code and not encounter_record.license_type_code:
                encounter_record.license_type_code = license_type_code
            if license_no and not encounter_record.license_no:
                encounter_record.license_no = license_no
            dx_row = EncounterDxRecord(
                encounter_no=encounter_no,
                dx_type_code=dx_type_code,
                kcd_code=kcd_code,
            )
            encounter_record.dx_list.append(dx_row)

    def _attach_items(self, encounters: Dict[str, EncounterRecord], path: Path) -> None:
        for line in self._iter_lines(path):
            encounter_no = self._slice_text(line, 1, 15)
            if not encounter_no:
                continue
            line_no = self._slice_text(line, 20, 4)
            item_code = self._slice_text(line, 25, 9)
            daily_amount = _parse_decimal(self._slice_text(line, 46, 7, strip=False), 2)
            days_str = self._slice_text(line, 53, 3)
            days = int(days_str) if days_str.isdigit() else None
            encounter_record = encounters.setdefault(encounter_no, EncounterRecord(encounter_no=encounter_no))
            item_record = EncounterItemRecord(
                encounter_no=encounter_no,
                line_no=line_no,
                item_code=item_code,
                daily_amount=daily_amount,
                days=days,
            )
            encounter_record.items.append(item_record)

    def _attach_details(self, encounters: Dict[str, EncounterRecord], path: Path) -> None:
        for line in self._iter_lines(path):
            encounter_no = self._slice_text(line, 1, 15)
            if not encounter_no:
                continue
            occurrence_scope = self._slice_text(line, 16, 1)
            if occurrence_scope != "2":
                # Only line-level details are required.
                continue
            line_no = self._slice_text(line, 17, 4)
            detail_text = self._slice_text(line, 26, 700, strip=False).rstrip()
            encounter_record = encounters.setdefault(encounter_no, EncounterRecord(encounter_no=encounter_no))
            target_item = next((item for item in encounter_record.items if item.line_no == line_no), None)
            if not target_item:
                logging.warning("Detail found without matching item: encounter=%s line=%s", encounter_no, line_no)
                continue
            target_item.add_detail_text(detail_text)

    def _parse_patient_file(
        self,
        path: Path,
        layout: ClaimFileLayout,
    ) -> Tuple[
        Dict[str, PatientRecord],
        Dict[str, InsuranceRecord],
        Dict[str, InvoiceRecord],
        Dict[str, Dict[str, Any]],
    ]:
        patients: Dict[str, PatientRecord] = {}
        insurances: Dict[str, InsuranceRecord] = {}
        invoices: Dict[str, InvoiceRecord] = {}
        encounter_meta: Dict[str, Dict[str, Any]] = {}
        for line in self._iter_lines(path):
            claim_no = self._slice_text(line, 1, 10)
            statement_no = self._slice_text(line, 11, 5)
            encounter_no = f"{claim_no}{statement_no}"
            patient_name = self._slice_text(line, layout.patient_name_start, layout.patient_name_length)
            identity_raw = self._slice_text(line, layout.patient_identity_start, layout.patient_identity_length)
            identity = identity_raw.replace("-", "")
            identity_prefix = identity[:6]
            identity_suffix = identity[6:]
            extra_fields: Dict[str, str] = {}
            if layout is AUTO_LAYOUT:
                extra_fields = {
                    "auto_accident_no": self._slice_text(line, 51, 30),
                    "auto_guarantee_no": self._slice_text(line, 81, 17),
                    "auto_visit_days": self._slice_text(line, 131, 3),
                    "auto_inpatient_days": self._slice_text(line, 134, 3),
                    "auto_result_code": self._slice_text(line, 137, 1),
                    "auto_total_cost": self._slice_text(line, 138, 10),
                    "auto_patient_payment": self._slice_text(line, 148, 10),
                    "auto_claim_amount": self._slice_text(line, 158, 10),
                    "auto_insurer_code": self._slice_text(line, 168, 2),
                }
            patient = PatientRecord(
                encounter_no=encounter_no,
                claim_no=claim_no,
                statement_no=statement_no,
                patient_name=patient_name,
                patient_identity_prefix=identity_prefix,
                patient_identity_suffix=identity_suffix,
                patient_gender=_parse_gender(identity_suffix),
                extra_fields=extra_fields,
            )
            patients[encounter_no] = patient
            if layout is MI_LAYOUT:
                medical_aid_code = self._slice_text(line, 39, 1)
                special_code = self._slice_text(line, 40, 1)
                copay_code = self._slice_text(line, 41, 1)
                insurance_record = InsuranceRecord(
                    encounter_no=encounter_no,
                    insurance_type_code=medical_aid_code,
                    insurance_nhis_type=_derive_insurance_nhis_type(medical_aid_code, special_code),
                    insurance_nhis_no=self._slice_text(line, 85, 20),
                    insurance_form_no=self._slice_text(line, 16, 4),
                    insurance_provider_code=self._slice_text(line, 20, 8),
                    insurance_payer_code=self._slice_text(line, 28, 11),
                    insurance_claim_type=self._slice_text(line, 42, 23),
                    insurance_subscriber_name=self._slice_text(line, 65, 20),
                    insurance_medical_aid_code=medical_aid_code,
                    insurance_special_code=special_code,
                    insurance_copay_code=copay_code,
                    claim_no=claim_no,
                    statement_no=statement_no,
                )
                insurances[encounter_no] = insurance_record
                invoice_record = InvoiceRecord(
                    encounter_no=encounter_no,
                    invoice_sum=_parse_amount(self._slice_text(line, 176, 10)),
                    invoice_insurance_sum=_parse_amount(self._slice_text(line, 176, 10)),
                    invoice_insurance_patient_burden=_parse_amount(self._slice_text(line, 186, 10)),
                    invoice_insurance_nhis_burden=_parse_amount(self._slice_text(line, 206, 10)),
                    invoice_subsidy=_parse_amount(self._slice_text(line, 216, 10)),
                    invoice_is_fixed_patient_burden=copay_code == "2",
                    invoice_patient_max_excess=_parse_amount(self._slice_text(line, 196, 10)),
                    invoice_disabled_support=_parse_amount(self._slice_text(line, 226, 10)),
                )
                invoices[encounter_no] = invoice_record
                encounter_meta[encounter_no] = {
                    "treatment_days": _parse_int(self._slice_text(line, 138, 3)),
                    "inpatient_days": _parse_int(self._slice_text(line, 141, 3)),
                    "result_code": self._slice_text(line, 175, 1),
                    "is_gongsang": special_code == "1",
                    "copay_type_code": copay_code,
                }
            elif layout is AUTO_LAYOUT:
                auto_insurer_code = self._slice_text(line, 168, 2)
                auto_company_name = AUTO_INSURER_COMPANIES.get(auto_insurer_code, "")
                insurance_record = InsuranceRecord(
                    encounter_no=encounter_no,
                    insurance_type_code="2",
                    insurance_form_no=self._slice_text(line, 16, 4),
                    insurance_provider_code=self._slice_text(line, 20, 8),
                    insurance_payer_code=auto_insurer_code,
                    insurance_claim_type=self._slice_text(line, 28, 23),
                    insurance_subscriber_name=patient_name,
                    insurance_ta_reg_no=self._slice_text(line, 51, 30),
                    insurance_ta_ins_no=self._slice_text(line, 81, 17),
                    insurance_ta_company_code=auto_insurer_code,
                    insurance_ta_company_name=auto_company_name,
                    claim_no=claim_no,
                    statement_no=statement_no,
                )
                insurances[encounter_no] = insurance_record
                invoice_record = InvoiceRecord(
                    encounter_no=encounter_no,
                    invoice_sum=_parse_amount(self._slice_text(line, 138, 10)),
                    invoice_insurance_sum=_parse_amount(self._slice_text(line, 138, 10)),
                    invoice_insurance_patient_burden=_parse_amount(self._slice_text(line, 148, 10)),
                    invoice_insurance_nhis_burden=_parse_amount(self._slice_text(line, 158, 10)),
                    invoice_subsidy=0,
                    invoice_is_fixed_patient_burden=False,
                )
                invoices[encounter_no] = invoice_record
                encounter_meta[encounter_no] = {
                    "treatment_days": _parse_int(self._slice_text(line, 131, 3)),
                    "inpatient_days": _parse_int(self._slice_text(line, 134, 3)),
                    "result_code": self._slice_text(line, 137, 1),
                    "is_gongsang": False,
                    "copay_type_code": "",
                }
        return patients, insurances, invoices, encounter_meta

    def _slice_text(self, line: bytes, start: int, length: int, *, strip: bool = True) -> str:
        begin = max(start - 1, 0)
        end = begin + length
        chunk = line[begin:end]
        text = chunk.decode(self.encoding, errors="ignore")
        if strip:
            return text.strip()
        return text

    def _iter_lines(self, path: Path) -> Iterable[bytes]:
        if not path.exists():
            logging.info("Skipping missing file %s", path)
            return []
        with path.open("rb") as handle:
            for raw_line in handle:
                yield raw_line.rstrip(b"\r\n")


def _export_csv(rows: List[Dict[str, str]], path: Path, *, encoding: str) -> None:
    if not rows:
        logging.info("No rows for %s, skipping", path.name)
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding=encoding) as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logging.info("Wrote %s (%d rows)", path, len(rows))


def export_results(
    encounters: Dict[str, EncounterRecord],
    output_dir: Path,
    *,
    output_encoding: str = OUTPUT_ENCODING_DEFAULT,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    sorted_encounters = [encounters[key] for key in sorted(encounters.keys())]
    patient_rows: List[Dict[str, str]] = []
    encounter_rows: List[Dict[str, str]] = []
    dx_rows: List[Dict[str, str]] = []
    item_rows: List[Dict[str, str]] = []
    insurance_rows: List[Dict[str, str]] = []
    invoice_rows: List[Dict[str, str]] = []

    for record in sorted_encounters:
        if record.patient:
            patient_rows.append(record.patient.to_row())
        encounter_rows.append(record.to_row())
        dx_rows.extend(dx.to_row() for dx in record.dx_list)
        item_rows.extend(item.to_row() for item in record.items)
        if record.insurance:
            insurance_rows.append(record.insurance.to_row())
        if record.invoice:
            invoice_rows.append(record.invoice.to_row())

    _export_csv(patient_rows, output_dir / "patients.csv", encoding=output_encoding)
    _export_csv(encounter_rows, output_dir / "encounters.csv", encoding=output_encoding)
    _export_csv(dx_rows, output_dir / "encounter_dx.csv", encoding=output_encoding)
    _export_csv(item_rows, output_dir / "encounter_items.csv", encoding=output_encoding)
    _export_csv(insurance_rows, output_dir / "insurances.csv", encoding=output_encoding)
    _export_csv(invoice_rows, output_dir / "invoices.csv", encoding=output_encoding)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse EDI K020.* claim files into CSV extracts")
    parser.add_argument("--source", default="data/test_source/mi", help="Root directory that holds YYYYMM claim folders")
    parser.add_argument("--output-dir", default="parsed_output", help="Destination directory for CSV files")
    parser.add_argument("--encoding", default=ENCODING_DEFAULT, help="Source file encoding (default: cp949)")
    parser.add_argument(
        "--output-encoding",
        default=OUTPUT_ENCODING_DEFAULT,
        help="Encoding for generated CSV files (default: cp949, use utf-8 for cross-platform)",
    )
    parser.add_argument("--log-level", default="INFO", help="Python logging level (default: INFO)")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s: %(message)s")
    source_path = Path(args.source)
    output_dir = Path(args.output_dir)
    claim_parser = EDIClaimParser(source_path, encoding=args.encoding)
    encounters = claim_parser.parse()
    export_results(encounters, output_dir, output_encoding=args.output_encoding)


if __name__ == "__main__":
    main()
