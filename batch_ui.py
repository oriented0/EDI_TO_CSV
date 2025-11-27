"""Minimal Tkinter UI for running the EDI claim parser over an entire folder tree."""

from __future__ import annotations

import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

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
        self.source_var = tk.StringVar(value=str(Path("data/test_source").resolve()))
        self.output_var = tk.StringVar(value=str(Path("parsed_output").resolve()))
        self.encoding_var = tk.StringVar(value=ENCODING_DEFAULT)
        self.output_encoding_var = tk.StringVar(value=OUTPUT_ENCODING_DEFAULT)
        self._build_layout()

    def _build_layout(self) -> None:
        padding = {"padx": 8, "pady": 4}

        tk.Label(self.root, text="Source folder").grid(row=0, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.source_var, width=60).grid(row=0, column=1, **padding)
        tk.Button(self.root, text="Browse", command=self._pick_source).grid(row=0, column=2, **padding)

        tk.Label(self.root, text="Output folder").grid(row=1, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.output_var, width=60).grid(row=1, column=1, **padding)
        tk.Button(self.root, text="Browse", command=self._pick_output).grid(row=1, column=2, **padding)

        tk.Label(self.root, text="Encoding").grid(row=2, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.encoding_var, width=20).grid(row=2, column=1, sticky="w", **padding)

        tk.Label(self.root, text="CSV encoding").grid(row=3, column=0, sticky="e", **padding)
        tk.Entry(self.root, textvariable=self.output_encoding_var, width=20).grid(row=3, column=1, sticky="w", **padding)

        self.run_button = tk.Button(self.root, text="Run Batch", command=self._run_batch)
        self.run_button.grid(row=4, column=0, columnspan=3, sticky="ew", padx=8, pady=8)

        self.log_widget = scrolledtext.ScrolledText(self.root, width=90, height=20, state="disabled")
        self.log_widget.grid(row=5, column=0, columnspan=3, padx=8, pady=8, sticky="nsew")

        self.root.rowconfigure(5, weight=1)
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

    def _initial_dir(self, candidate: str) -> str:
        path = Path(candidate)
        if path.exists():
            return str(path)
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
            self._log(f"발견한 청구 폴더: {len(claim_dirs)}개")
            for path, layout in claim_dirs:
                layout_label = "K020" if layout.patient_file.startswith("K020") else "C110"
                self._log(f" - [{layout_label}] {path}")
            encounters = parser.parse()
            export_results(encounters, output, output_encoding=self.output_encoding_var.get())
            self._log(f"완료: {len(encounters)}건 처리, 결과: {output}")
            self.root.after(0, lambda: messagebox.showinfo("완료", f"총 {len(encounters)}건 처리했습니다."))
        except Exception as exc:  # noqa: BLE001
            self._log(f"오류 발생: {exc}")
            self.root.after(0, lambda: messagebox.showerror("오류", str(exc)))
        finally:
            self.root.after(0, lambda: self.run_button.config(state="normal"))

    def _log(self, message: str) -> None:
        def append() -> None:
            self.log_widget.configure(state="normal")
            self.log_widget.insert(tk.END, f"{message}\n")
            self.log_widget.see(tk.END)
            self.log_widget.configure(state="disabled")

        self.root.after(0, append)

    def run(self) -> None:
        self.root.mainloop()


def main() -> None:
    BatchParserUI().run()


if __name__ == "__main__":
    main()
