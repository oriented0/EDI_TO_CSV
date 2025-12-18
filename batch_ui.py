"""Minimal Tkinter UI for running the EDI claim parser over an entire folder tree."""

from __future__ import annotations

import locale
import os
import subprocess
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext
from typing import Dict, List, Tuple

from edi_parser import (
    ENCODING_DEFAULT,
    OUTPUT_ENCODING_DEFAULT,
    EDIClaimParser,
    export_results,
)


class BatchParserUI:
    def __init__(self) -> None:
        self.root = tk.Tk()
        self.root.title("EDI Batch Parser")
        self.app_root = Path.cwd()
        self.run_commands_exe = self.app_root / "run_commands.exe"
        patient_default = self.app_root / "db_injection" / "patient" / "patient.csv"
        edi_default = self.app_root / "db_injection" / "edi"
        self.source_var = tk.StringVar(value=str(Path("C:/hira/DDMD/data/DMD").resolve()))
        self.output_var = tk.StringVar(value=str(Path("parsed_output").resolve()))
        self.encoding_var = tk.StringVar(value=ENCODING_DEFAULT)
        self.output_encoding_var = tk.StringVar(value=OUTPUT_ENCODING_DEFAULT)
        self.patient_csv_var = tk.StringVar(value=str(patient_default.resolve()))
        self.edi_dir_var = tk.StringVar(value=str(edi_default.resolve()))
        preferred_encoding = locale.getpreferredencoding(False) or "cp949"
        self.subprocess_encoding = "utf-8"
        self.subprocess_encoding_fallback = preferred_encoding
        self._build_layout()

    def _build_layout(self) -> None:
        padding = {"padx": 8, "pady": 4}

        tk.Label(self.root, text="Source folder").grid(row=0, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.source_var, width=60).grid(row=0, column=1, columnspan=2, sticky="we", **padding)
        tk.Button(self.root, text="Browse", command=self._pick_source).grid(row=0, column=3, **padding)

        tk.Label(self.root, text="Output folder").grid(row=1, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.output_var, width=60).grid(row=1, column=1, columnspan=2, sticky="we", **padding)
        tk.Button(self.root, text="Browse", command=self._pick_output).grid(row=1, column=3, **padding)

        tk.Label(self.root, text="Encoding").grid(row=2, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.encoding_var, width=20).grid(row=2, column=1, sticky="w", **padding)

        tk.Label(self.root, text="CSV encoding").grid(row=3, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.output_encoding_var, width=20).grid(row=3, column=1, sticky="w", **padding)

        self.run_button = tk.Button(self.root, text="Run Batch", command=self._run_batch)
        self.run_button.grid(row=4, column=0, columnspan=4, sticky="ew", padx=8, pady=8)

        tk.Label(self.root, text="환자 CSV").grid(row=5, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.patient_csv_var, width=60).grid(row=5, column=1, sticky="we", **padding)
        tk.Button(self.root, text="Browse", command=self._pick_patient_csv).grid(row=5, column=2, **padding)
        self.patient_transfer_button = tk.Button(self.root, text="환자정보 전송", command=self._run_patient_transfer)
        self.patient_transfer_button.grid(row=5, column=3, sticky="ew", **padding)

        tk.Label(self.root, text="EDI 폴더").grid(row=6, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.edi_dir_var, width=60).grid(row=6, column=1, sticky="we", **padding)
        tk.Button(self.root, text="Browse", command=self._pick_edi_folder).grid(row=6, column=2, **padding)
        self.edi_transfer_button = tk.Button(self.root, text="진료정보 전송", command=self._run_edi_transfer)
        self.edi_transfer_button.grid(row=6, column=3, sticky="ew", **padding)

        self.log_widget = scrolledtext.ScrolledText(self.root, width=100, height=18, state="disabled")
        self.log_widget.grid(row=7, column=0, columnspan=4, padx=8, pady=8, sticky="nsew")

        self.root.rowconfigure(7, weight=1)
        self.root.columnconfigure(1, weight=1)

    def _pick_source(self) -> None:
        selection = filedialog.askdirectory(
            title="Select claim root folder",
            initialdir=self._initial_dir(self.source_var.get()),
        )
        if selection:
            self.source_var.set(selection)

    def _pick_output(self) -> None:
        selection = filedialog.askdirectory(
            title="Select output folder",
            initialdir=self._initial_dir(self.output_var.get()),
        )
        if selection:
            self.output_var.set(selection)

    def _pick_patient_csv(self) -> None:
        selection = filedialog.askopenfilename(
            title="환자 CSV 파일 선택",
            initialdir=self._initial_dir(self.patient_csv_var.get()),
            filetypes=(("CSV", "*.csv"), ("All files", "*.*")),
        )
        if selection:
            self.patient_csv_var.set(selection)

    def _pick_edi_folder(self) -> None:
        selection = filedialog.askdirectory(
            title="EDI 폴더 선택",
            initialdir=self._initial_dir(self.edi_dir_var.get()),
        )
        if selection:
            self.edi_dir_var.set(selection)

    def _initial_dir(self, candidate: str) -> str:
        if not candidate:
            return str(Path.cwd())
        path = Path(candidate)
        if path.is_file():
            return str(path.parent)
        if path.is_dir():
            return str(path)
        parent = path.parent
        if parent.is_dir():
            return str(parent)
        return str(Path.cwd())

    def _run_batch(self) -> None:
        source = self.source_var.get().strip()
        output = self.output_var.get().strip()
        if not source:
            messagebox.showwarning("Missing source", "Please choose a source folder")
            return
        if not output:
            messagebox.showwarning("Missing output", "Please choose an output folder")
            return
        self.run_button.config(state="disabled")
        self._log("Starting batch run…")
        thread = threading.Thread(
            target=self._execute_batch,
            args=(Path(source), Path(output)),
            daemon=True,
        )
        thread.start()

    def _execute_batch(self, source: Path, output: Path) -> None:
        try:
            parser = EDIClaimParser(source, encoding=self.encoding_var.get())
            claim_dirs = parser.discover_claim_dirs()
            if not claim_dirs:
                raise FileNotFoundError("해당 폴더에서 K020/C110 파일을 찾을 수 없습니다.")
            empty_children = self._find_empty_children(source, claim_dirs)
            if empty_children:
                self._log("추출 대상이 없는 하위 폴더:")
                for child in empty_children:
                    self._log(f" - {child}")
            encounters, successes, failures, month_map = parser.parse_with_status()
            self._log(f"추출 완료 폴더: {len(successes)}개")
            if failures:
                self._log(f"실패한 폴더: {len(failures)}개")
                for path, reason in failures.items():
                    self._log(f" - {path}: {reason}")
            self._log("청구월 요약:")
            summary_lines = self._format_month_summary(month_map)
            for line in summary_lines:
                self._log(f" - {line}")
            export_results(encounters, output, output_encoding=self.output_encoding_var.get())
            self._log(
                f"완료: 총 {len(encounters)}건 처리 (성공 {len(successes)} 폴더, 실패 {len(failures)} 폴더). 결과: {output}"
            )
            self.root.after(
                0,
                lambda: messagebox.showinfo(
                    "완료",
                    f"총 {len(encounters)}건 처리했고, {len(successes)}개 폴더 성공 / {len(failures)}개 폴더 실패했습니다.",
                ),
            )
        except Exception as exc:  # noqa: BLE001
            self._log(f"오류 발생: {exc}")
            self.root.after(0, lambda: messagebox.showerror("오류", str(exc)))
        finally:
            self.root.after(0, lambda: self.run_button.config(state="normal"))

    def _run_patient_transfer(self) -> None:
        csv_path = Path(self.patient_csv_var.get().strip())
        if not csv_path.is_file():
            messagebox.showwarning("파일 없음", f"환자 CSV 파일을 찾을 수 없습니다:\n{csv_path}")
            return
        self.patient_transfer_button.config(state="disabled")
        thread = threading.Thread(target=self._execute_patient_transfer, args=(csv_path,), daemon=True)
        thread.start()

    def _execute_patient_transfer(self, csv_path: Path) -> None:
        try:
            self._log(f"환자정보 전송 시작: {csv_path}")
            self._run_run_commands(["import_patients_from_csv", str(csv_path)], "환자정보 전송")
            self.root.after(0, lambda: messagebox.showinfo("완료", "환자정보 전송을 마쳤습니다."))
        except Exception as exc:  # noqa: BLE001
            self._log(f"환자정보 전송 실패: {exc}")
            self.root.after(0, lambda: messagebox.showerror("오류", f"환자정보 전송 실패: {exc}"))
        finally:
            self.root.after(0, lambda: self.patient_transfer_button.config(state="normal"))

    def _run_edi_transfer(self) -> None:
        edi_dir = Path(self.edi_dir_var.get().strip())
        if not edi_dir.is_dir():
            messagebox.showwarning("폴더 없음", f"청구 EDI 폴더를 찾을 수 없습니다:\n{edi_dir}")
            return
        self.edi_transfer_button.config(state="disabled")
        thread = threading.Thread(target=self._execute_edi_transfer, args=(edi_dir,), daemon=True)
        thread.start()

    def _execute_edi_transfer(self, edi_dir: Path) -> None:
        try:
            self._log(f"진료정보 전송 시작: {edi_dir}")
            self._run_run_commands(["import_edi_csv", "--base-dir", str(edi_dir)], "진료정보 전송")
            self.root.after(0, lambda: messagebox.showinfo("완료", "진료정보 전송을 마쳤습니다."))
        except Exception as exc:  # noqa: BLE001
            self._log(f"진료정보 전송 실패: {exc}")
            self.root.after(0, lambda: messagebox.showerror("오류", f"진료정보 전송 실패: {exc}"))
        finally:
            self.root.after(0, lambda: self.edi_transfer_button.config(state="normal"))

    def _log(self, message: str) -> None:
        def append() -> None:
            self.log_widget.configure(state="normal")
            self.log_widget.insert(tk.END, f"{message}\n")
            self.log_widget.see(tk.END)
            self.log_widget.configure(state="disabled")

        self.root.after(0, append)

    def _run_run_commands(self, args: List[str], label: str) -> None:
        exe = self.run_commands_exe
        if not exe.exists():
            raise FileNotFoundError(f"run_commands.exe 경로를 찾을 수 없습니다: {exe}")
        cmd = [str(exe), *[str(arg) for arg in args]]
        pretty = " ".join(self._quote_arg(part) for part in cmd)
        self._log(f"{label} 실행: {pretty}")
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding=self.subprocess_encoding,
                errors="ignore",
                env=env,
            )
        except FileNotFoundError as exc:  # pragma: no cover - surface user-friendly message
            raise FileNotFoundError("run_commands.exe 실행에 실패했습니다.") from exc
        if result.stdout:
            for line in result.stdout.strip().splitlines():
                if line:
                    self._log(f"[stdout] {line}")
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                if line:
                    self._log(f"[stderr] {line}")
        if result.returncode != 0:
            raise RuntimeError(f"{label} 실패 (코드 {result.returncode})")
        self._log(f"{label} 완료")

    def _find_empty_children(self, source: Path, claim_dirs: List[Tuple[Path, object]]) -> List[Path]:
        claim_paths = [path for path, _ in claim_dirs]
        empty: List[Path] = []
        try:
            children = [child for child in source.iterdir() if child.is_dir()]
        except FileNotFoundError:
            return empty
        for child in children:
            has_claim = any(self._is_subpath(claim_path, child) for claim_path in claim_paths)
            if not has_claim:
                empty.append(child)
        return empty

    @staticmethod
    def _is_subpath(path: Path, parent: Path) -> bool:
        try:
            path.relative_to(parent)
            return True
        except ValueError:
            return False

    @staticmethod
    def _format_month_summary(month_map: Dict[str, List[str]]) -> List[str]:
        buckets: Dict[str, List[str]] = {"건보": [], "자보": []}
        for kind, months in month_map.items():
            buckets.setdefault(kind, []).extend(months)
        lines: List[str] = []
        for kind in ["건보", "자보"]:
            months = sorted(set(buckets.get(kind, [])))
            if not months:
                continue
            year_groups: Dict[str, List[str]] = {}
            for month in months:
                if "." in month:
                    year = month.split(".", 1)[0]
                else:
                    year = "알수없음"
                year_groups.setdefault(year, []).append(month)
            indent = " " * (len(kind) + 3)
            first = True
            for year in sorted(year_groups.keys()):
                segment = " ".join(year_groups[year])
                if first:
                    lines.append(f"{kind} : {segment}")
                    first = False
                else:
                    lines.append(f"{indent}{segment}")
        if not lines:
            lines.append("청구월 정보를 찾을 수 없습니다.")
        return lines

    @staticmethod
    def _quote_arg(arg: str) -> str:
        if " " in arg:
            return f'"{arg}"'
        return arg

    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    BatchParserUI().run()


if __name__ == "__main__":
    main()
