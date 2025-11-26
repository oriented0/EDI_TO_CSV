"""Batch processor for decrypting DDMD payloads and refreshing CSV exports.

The script automates the following workflow:

1. Enumerate encrypted batches that live under ``C:\hira\DDMD\data\DMD``.
2. For every batch (oldest first), copy the ``*.enc`` payload into
   ``C:\hira\DDMD\sam\in`` and invoke ``dec.exe cipherdec default`` so that
   the official DDMD client performs the decryption.
3. Immediately copy the decrypted K020.* (health insurance) or C110.*
   (auto-insurance) files out of ``sam\in`` and store them under
   ``decoded_batches/<batch_id>`` inside this repository.
4. Once all new batches are copied locally, reuse ``edi_parser.EDIClaimParser``
   to rebuild the patients / encounters CSV snapshots.

Re-running the script is safe: batches that already exist under
``decoded_batches`` are skipped, which means only newly-arrived directories in
``data\DMD`` trigger additional decode operations.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

from edi_parser import (
    ENCODING_DEFAULT,
    OUTPUT_ENCODING_DEFAULT,
    EDIClaimParser,
    export_results,
)

DDMD_DATA_ROOT = Path(r"C:\hira\DDMD\data\DMD")
DDMD_SAM_IN = Path(r"C:\hira\DDMD\sam\in")
DDMD_DEC_EXE = Path(r"C:\hira\DDMD\bin\dec.exe")
SUPPORTED_OUTPUTS = (
    "H010",
    "K020.1",
    "K020.2",
    "K020.3",
    "K020.4",
    "C010",
    "C110.1",
    "C110.2",
    "C110.3",
    "C110.4",
)


@dataclass(frozen=True)
class BatchInfo:
    doc_id: str
    source_path: Path
    staging_name: str
    sort_key: float


def _pick_staging_name(path: Path) -> str:
    if path.name.lower().endswith(".enc"):
        return path.name
    return f"{path.name}.enc"


def discover_batches(data_root: Path, *, use_enc_fallback: bool = True) -> List[BatchInfo]:
    batches: List[BatchInfo] = []
    for child in data_root.iterdir():
        if not child.is_dir():
            continue
        payload: Path | None = None
        staging_name: str | None = None

        zip_dir = child / "zip"
        enc_dir = child / "enc"

        if zip_dir.exists():
            candidates = list(zip_dir.glob("*.enc"))
            if candidates:
                payload = sorted(candidates, key=lambda p: p.stat().st_mtime)[0]
                staging_name = payload.name

        if payload is None and use_enc_fallback and enc_dir.exists():
            candidates = [p for p in enc_dir.iterdir() if p.is_file() and not p.suffix.lower() == ".sig"]
            if candidates:
                payload = sorted(candidates, key=lambda p: p.stat().st_mtime)[0]
                staging_name = _pick_staging_name(payload)

        if payload is None or staging_name is None:
            continue

        batches.append(
            BatchInfo(
                doc_id=child.name,
                source_path=payload,
                staging_name=staging_name,
                sort_key=payload.stat().st_mtime,
            ),
        )
    return sorted(batches, key=lambda info: info.sort_key)


def copy_decoded_files(sam_in: Path, destination: Path) -> List[Path]:
    destination.mkdir(parents=True, exist_ok=True)
    copied: List[Path] = []
    for filename in SUPPORTED_OUTPUTS:
        src = sam_in / filename
        if src.exists():
            target = destination / filename
            shutil.copy2(src, target)
            copied.append(target)
    return copied


def cleanup_staging_payloads(sam_in: Path) -> None:
    for pattern in ("*.enc", "*.ENC", "*.enc.ZIP", "*.ENC.ZIP", "*.ZIP", "*.zip"):
        for path in sam_in.glob(pattern):
            try:
                path.unlink()
            except (FileNotFoundError, PermissionError):
                continue


def run_decoding(dec_executable: Path, sam_in: Path) -> bool:
    command = f'"{dec_executable}" cipherdec default'
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logging.warning("dec.exe exited with code %s; validating via DecResult.txt", result.returncode)
        logging.debug("stdout: %s", result.stdout.strip())
        logging.debug("stderr: %s", result.stderr.strip())
    dec_result = sam_in / "DecResult.txt"
    if not dec_result.exists():
        logging.error("DecResult.txt was not produced; cannot confirm success")
        return False
    status = dec_result.read_text(encoding="cp949").strip()
    if status != "1":
        logging.error("dec.exe reported failure status %s", status)
        logging.error("stdout: %s", result.stdout.strip())
        logging.error("stderr: %s", result.stderr.strip())
        return False
    return True


def process_batches(
    *,
    data_root: Path,
    sam_in: Path,
    dec_exe: Path,
    decoded_root: Path,
    max_batches: int | None = None,
    use_enc_fallback: bool = True,
) -> Sequence[Path]:
    decoded_root.mkdir(parents=True, exist_ok=True)
    batches = discover_batches(data_root, use_enc_fallback=use_enc_fallback)
    logging.info("Found %d batch directories under %s", len(batches), data_root)
    new_batch_dirs: List[Path] = []

    cleanup_staging_payloads(sam_in)
    processed = 0
    for batch in batches:
        if max_batches and processed >= max_batches:
            break
        batch_dest = decoded_root / batch.doc_id
        if (batch_dest / "K020.1").exists() or (batch_dest / "C110.1").exists():
            logging.debug("Skipping already-decoded batch %s", batch.doc_id)
            continue
        logging.info("Decoding batch %s", batch.doc_id)
        inbound_payload = sam_in / batch.staging_name
        if inbound_payload.exists():
            inbound_payload.unlink()
        shutil.copy2(batch.source_path, inbound_payload)
        success = run_decoding(dec_exe, sam_in)
        cleanup_staging_payloads(sam_in)
        if not success:
            logging.error("Skipping batch %s due to dec.exe failure", batch.doc_id)
            processed += 1
            continue
        copied_files = copy_decoded_files(sam_in, batch_dest)
        if not copied_files:
            logging.warning("No claim files were copied for batch %s", batch.doc_id)
        else:
            logging.info("Copied %d claim files for batch %s", len(copied_files), batch.doc_id)
            new_batch_dirs.append(batch_dest)
        processed += 1
    return new_batch_dirs


def rebuild_csv_exports(decoded_root: Path, output_dir: Path, *, encoding: str, output_encoding: str) -> None:
    claim_parser = EDIClaimParser(decoded_root, encoding=encoding)
    encounters = claim_parser.parse()
    export_results(encounters, output_dir, output_encoding=output_encoding)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Decode DDMD batches and refresh CSV outputs")
    parser.add_argument("--data-root", default=DDMD_DATA_ROOT, type=Path, help="Path to C\\hira\\DDMD\\data\\DMD")
    parser.add_argument("--sam-in", default=DDMD_SAM_IN, type=Path, help="Path to C\\hira\\DDMD\\sam\\in")
    parser.add_argument("--dec-exe", default=DDMD_DEC_EXE, type=Path, help="Path to C\\hira\\DDMD\\bin\\dec.exe")
    parser.add_argument("--decoded-root", default=Path("decoded_batches"), type=Path, help="Folder to store decrypted claim files")
    parser.add_argument("--output-dir", default=Path("parsed_output"), type=Path, help="Destination for refreshed CSV files")
    parser.add_argument("--encoding", default=ENCODING_DEFAULT, help="Source file encoding (default: cp949)")
    parser.add_argument(
        "--output-encoding",
        default=OUTPUT_ENCODING_DEFAULT,
        help="Encoding to use for CSV exports (default: cp949)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Limit the number of batches to process (0 = no limit)",
    )
    parser.add_argument(
        "--zip-only",
        action="store_true",
        help="Only consider payloads that already have .enc files under the zip directory",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s: %(message)s")
    new_dirs = process_batches(
        data_root=args.data_root,
        sam_in=args.sam_in,
        dec_exe=args.dec_exe,
        decoded_root=args.decoded_root,
        max_batches=args.max_batches or None,
        use_enc_fallback=not args.zip_only,
    )
    if new_dirs:
        logging.info("Decoded %d new batch(es)", len(new_dirs))
    else:
        logging.info("No new batches required decoding; regenerating CSVs from existing material")
    rebuild_csv_exports(
        decoded_root=args.decoded_root,
        output_dir=args.output_dir,
        encoding=args.encoding,
        output_encoding=args.output_encoding,
    )


if __name__ == "__main__":
    main()
