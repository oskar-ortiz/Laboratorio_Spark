from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
XLSX_PATH = RAW_DIR / "Online Retail.xlsx"
CSV_PATH = RAW_DIR / "online_retail.csv"
EXPECTED_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if not XLSX_PATH.exists():
        raise FileNotFoundError(f"No se encontro el archivo original: {XLSX_PATH}")

    source = pd.read_excel(XLSX_PATH, engine="openpyxl", dtype={"InvoiceNo": str, "StockCode": str})
    if list(source.columns) != EXPECTED_COLUMNS:
        raise ValueError(f"Columnas inesperadas: {list(source.columns)}")

    source["InvoiceNo"] = source["InvoiceNo"].astype(str)
    source["StockCode"] = source["StockCode"].astype(str)
    source.to_csv(CSV_PATH, index=False, encoding="utf-8")
    print(f"Dataset preparado: {CSV_PATH}")
    print(f"Filas: {len(source)}")
    print(f"Columnas: {len(source.columns)}")
    print(f"Facturas con prefijo C: {source['InvoiceNo'].str.startswith('C').sum()}")


if __name__ == "__main__":
    main()
