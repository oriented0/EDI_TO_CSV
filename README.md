# EDI Claim Parser

Utilities for reconstructing patient and encounter level data from fixed-width
EDI claim files. The script understands both the 건강보험 K020._ layout (for
example, `test_source/mi/YYYYMM`) and the 자동차 보험 C110._ layout (for
example, `test_source/ta/YYYYMM`) described in the root instructions text file.

## Requirements

- Python 3.10+ (the project was tested with 3.11).
- No third-party libraries are required.

## Running the parser

```bash
python edi_parser.py --source test_source --output-dir parsed_output
```

Command-line options:

- `--source`: Root directory that contains month folders with `K020.*` or
  `C110.*` files. Defaults to `test_source/mi`.
- `--output-dir`: Directory that will receive the generated CSV exports.
  Defaults to `parsed_output` (the folder is created automatically).
- `--encoding`: Source file encoding. `cp949` works for the bundled data.
- `--output-encoding`: Encoding for the generated CSV files. Defaults to
  `cp949`, which keeps Korean characters legible in Microsoft Excel on
  Windows. Use `utf-8` when sharing with non-Windows tooling.
- `--log-level`: Standard Python logging level (INFO, DEBUG, ...).

The parser inspects every folder under `--source` that contains either a
`K020.1` or `C110.1` file and reads the companion `*.2`, `*.3`, and `*.4` files
when present. Missing files are skipped with an informational log entry.
`patients.csv` retains just the identifying fields (name, 주민번호 앞/뒤 자리,
성별) together with `encounter_no`, so downstream systems can join patients to
encounters while generating fresh `encounter_uuid` values inside the database.

## Output files

Running the parser produces four CSV files:

| File                  | Contents                                                                                                                                                                                  |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `patients.csv`        | Minimal patient slice per encounter (encounter number, name, identity prefix/suffix, gender).                                                                                             |
| `encounters.csv`      | Minimal encounter slice (encounter number, patient link = same encounter number, encounter date, doctor license number, boolean flags for related dx/items).                              |
| `encounter_dx.csv`    | Diagnosis list per encounter containing only the linkage key, primary/secondary flag, and KCD code.                                                                                       |
| `encounter_items.csv` | Itemized rows keyed by encounter number and line number, containing the linkage columns, `item_code`, `daily_amount`, `days`, and (when applicable) the merged detail text from `K020.4`. |

All numeric amounts are rendered with two decimal places. Dates use the ISO
format (`YYYY-MM-DD`).

## Field mapping summary

| Source file | Columns (1-based) | Target field                                                                                           |
| ----------- | ----------------- | ------------------------------------------------------------------------------------------------------ |
| `K020.1`    | 1-10              | Claim number (`claim_no`).                                                                             |
| `K020.1`    | 11-15             | Statement sequence (`statement_no`).                                                                   |
| `K020.1`    | 105-124           | Patient name (`patient_name`).                                                                         |
| `K020.1`    | 125-137           | Resident registration number (split into prefix/suffix and gender derived from the suffix lead digit). |
| `C110.1`    | 1-10              | Claim number (`claim_no`).                                                                             |
| `C110.1`    | 11-15             | Statement sequence (`statement_no`).                                                                   |
| `C110.1`    | 98-117            | Patient name (`patient_name`).                                                                         |
| `C110.1`    | 118-130           | Resident registration number (split into prefix/suffix and gender derived from the suffix lead digit). |
| `K020.2`    | 1-15              | Encounter number (links to every other file).                                                          |
| `K020.2`    | 16                | Diagnosis type (`1` primary, `2` secondary).                                                           |
| `K020.2`    | 17-22             | KCD diagnosis code.                                                                                    |
| `K020.2`    | 23-24             | Department code.                                                                                       |
| `K020.2`    | 25-32             | Encounter date.                                                                                        |
| `K020.2`    | 33                | Doctor license type (`3` = oriental doctor).                                                           |
| `K020.2`    | 34-43             | Doctor license number.                                                                                 |
| `K020.3`    | 1-15              | Encounter number.                                                                                      |
| `K020.3`    | 20-23             | Line number (combined with encounter number for a unique item key).                                    |
| `K020.3`    | 25-33             | Service code (`encounter_item_code`).                                                                  |
| `K020.3`    | 46-52             | Daily amount (5 digits + 2 implied decimals).                                                          |
| `K020.3`    | 53-55             | Day count.                                                                                             |
| `K020.4`    | 1-15              | Encounter number (only scope `2` rows are used).                                                       |
| `K020.4`    | 16                | Occurrence scope (`2`=line).                                                                           |
| `K020.4`    | 17-20             | Line number.                                                                                           |
| `K020.4`    | 26-725            | Free-text detail (stored as `detail_text` inside `encounter_items.csv`).                               |

`C110.2`, `C110.3`, and `C110.4` share the same column layout as `K020.2`,
`K020.3`, and `K020.4` respectively; only the patient master offsets differ.

These offsets assume byte positions in the CP949-encoded files. The parser
works directly with byte slices to ensure Korean characters align with the
expected columns.

## Extending the parser

- Additional claim types (e.g., `ta`) can be supported by pointing `--source`
  to the corresponding folder as long as `K020.*` layouts remain consistent.
- The `export_results` helper consolidates the parsed structures into CSV files.
  If you need JSON or database insertion scripts, reuse the `EncounterRecord`
  tree produced by `EDIClaimParser.parse()`.
- For sensitive deployments, adjust how resident registration numbers are
  handled inside `PatientRecord` before writing to CSV (e.g., encrypt instead of
  emitting the raw prefix/suffix columns).
