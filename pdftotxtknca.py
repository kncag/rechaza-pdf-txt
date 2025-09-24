# streamlit_app.py
from __future__ import annotations
import io
import re
import zipfile
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import streamlit as st
import pandas as pd

# Optional import for PDF text extraction
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

# -----------------------
# Configuration / Constants
# -----------------------
# TXT fixed-width positions (1-based inclusive)
TXT_POS = {
    "dni": (25, 33),
    "nombre": (40, 85),
    "referencia": (115, 126),
    "importe": (186, 195),
}

ESTADO_FIJO = "rechazada"
MULTIPLICADOR = 2  # fixed internal multiplier for PDF->TXT flow

# Rejection options for PDF->TXT flow (user selectable)
PDF_RECHAZOS: Dict[str, Tuple[str, str]] = {
    "R002: CUENTA INVALIDA": ("R002", "CUENTA INVALIDA"),
    "R001: DOCUMENTO ERRADO": ("R001", "DOCUMENTO ERRADO"),
}

# Rejection logic for ZIP->Excel flow (based on column O)
RECHAZO_R016 = ("R016", "CLIENTE NO TITULAR DE LA CUENTA")
RECHAZO_R002 = ("R002", "CUENTA INVALIDA")
KEYWORDS_NO_TITULAR = [
    "no es titular", "beneficiario no", "cliente no titular", "no titular",
    "continuar", "puedes continuar", "si deseas", "si deseas, puedes continuar"
]

# Columns ordering for output
OUT_COLUMNS = [
    "dni/cex",
    "nombre",
    "importe",
    "Referencia",
    "Estado",
    "Codigo de Rechazo",
    "Descripcion de Rechazo",
]

# Logging
logger = logging.getLogger("rechazos_app")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# -----------------------
# Utility functions
# -----------------------
def parse_amount(raw: Optional[str]) -> float:
    """Normalize amount strings to float. Returns 0.0 on failure."""
    if raw is None:
        return 0.0
    s = str(raw).strip()
    s = re.sub(r"[^\d,.-]", "", s)
    if not s:
        return 0.0
    # Heuristics: if both separators present, assume '.' thousands and ',' decimal
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    # Keep last dot as decimal if multiple
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except ValueError:
        return 0.0

def slice_fixed(line: str, start_1: int, end_1: int) -> str:
    """Return substring from 1-based inclusive positions; safe for short lines."""
    if line is None:
        return ""
    start = max(0, start_1 - 1)
    return line[start:end_1].strip() if start < len(line) else ""

def contains_no_titular(text: Optional[str]) -> bool:
    if not isinstance(text, str):
        return False
    t = text.lower()
    return any(k in t for k in KEYWORDS_NO_TITULAR)

def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Rechazos") -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buf.getvalue()

# -----------------------
# PDF -> TXT flow helpers
# -----------------------
def extract_registros_from_pdf_bytes(pdf_bytes: bytes) -> List[int]:
    """Extract unique Registro N (up to 5 digits) from entire PDF text."""
    if fitz is None:
        raise RuntimeError("PyMuPDF (fitz) not available in environment")
    text = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            text.append(page.get_text() or "")
    joined = "\n".join(text)
    matches = re.findall(r"Registro\s+(\d{1,5})", joined, re.IGNORECASE)
    nums = sorted({int(m) for m in matches})
    logger.info("Found registros in PDF: %s", nums)
    return nums

def read_txt_lines(txt_bytes: bytes, encoding: str = "utf-8") -> List[str]:
    s = txt_bytes.decode(encoding, errors="replace")
    return [ln.rstrip("\r\n") for ln in s.splitlines()]

def build_rows_from_txt_indices(indices: List[int], txt_lines: List[str], codigo: str, descripcion: str) -> List[Dict]:
    rows = []
    total = len(txt_lines)
    for idx in indices:
        if idx < 1 or idx > total:
            # Out of bounds: produce empty row with importe 0
            dni = nombre = referencia = ""
            importe_val = 0.0
        else:
            line = txt_lines[idx - 1]
            dni = slice_fixed(line, *TXT_POS["dni"])
            nombre = slice_fixed(line, *TXT_POS["nombre"])
            referencia = slice_fixed(line, *TXT_POS["referencia"])
            importe_raw = slice_fixed(line, *TXT_POS["importe"])
            importe_val = parse_amount(importe_raw)
        rows.append({
            "dni/cex": dni,
            "nombre": nombre,
            "importe": importe_val,
            "Referencia": referencia,
            "Estado": ESTADO_FIJO,
            "Codigo de Rechazo": codigo,
            "Descripcion de Rechazo": descripcion,
        })
    return rows

# -----------------------
# ZIP -> Excel flow helpers
# -----------------------
def read_first_excel_from_zip_bytes(zip_bytes: bytes) -> pd.DataFrame:
    """Open first .xlsx/.xls in zip and return first sheet as DataFrame (strings)."""
    buf = io.BytesIO(zip_bytes)
    with zipfile.ZipFile(buf) as z:
        candidates = [n for n in z.namelist() if n.lower().endswith((".xlsx", ".xls"))]
        if not candidates:
            raise ValueError("No Excel file found in ZIP")
        first = candidates[0]
        with z.open(first) as f:
            df = pd.read_excel(f, sheet_name=0, dtype=str)
    logger.info("Loaded Excel from ZIP: %s rows x %s cols", df.shape[0], df.shape[1])
    return df

def get_col_by_letter_from_row(row: pd.Series, letter: str) -> str:
    idx = ord(letter.upper()) - ord("A")
    vals = list(row.values)
    if idx < 0 or idx >= len(vals):
        return ""
    val = vals[idx]
    return "" if pd.isna(val) else str(val).strip()

def build_rows_from_excel_df(df: pd.DataFrame, start_row: int = 12) -> List[Dict]:
    """
    Process Excel DataFrame starting from 1-based start_row (default 12).
    Include only rows where column O has content. Map columns by letter:
      E -> dni, F -> nombre, N -> importe, H -> Referencia, O -> condition.
    """
    rows = []
    if df.shape[0] < start_row:
        return rows
    df_proc = df.iloc[start_row - 1 :].reset_index(drop=True)  # start_row is 1-based
    for _, row in df_proc.iterrows():
        col_o = get_col_by_letter_from_row(row, "O")
        if not col_o:
            continue
        dni = get_col_by_letter_from_row(row, "E")
        nombre = get_col_by_letter_from_row(row, "F")
        importe_raw = get_col_by_letter_from_row(row, "N")
        referencia = get_col_by_letter_from_row(row, "H")
        importe_val = parse_amount(importe_raw)
        if contains_no_titular(col_o):
            codigo, descripcion = RECHAZO_R016
        else:
            codigo, descripcion = RECHAZO_R002
        rows.append({
            "dni/cex": dni,
            "nombre": nombre,
            "importe": importe_val,
            "Referencia": referencia,
            "Estado": ESTADO_FIJO,
            "Codigo de Rechazo": codigo,
            "Descripcion de Rechazo": descripcion,
        })
    logger.info("Built %d rows from Excel", len(rows))
    return rows

# -----------------------
# Streamlit UI - Modular
# -----------------------
def pdf_txt_tab() -> None:
    st.header("Flujo PDF → TXT")
    st.write("Sube PDF y TXT. Extrae 'Registro N' del PDF, multiplica por 2 (interno), busca la línea en el TXT y genera el Excel.")
    selection = st.selectbox("Código de Rechazo (PDF→TXT)", list(PDF_RECHAZOS.keys()))
    codigo, descripcion = PDF_RECHAZOS[selection]
    col1, col2 = st.columns(2)
    with col1:
        pdf_file = st.file_uploader("Sube PDF", type="pdf", key="pdf_flow")
    with col2:
        txt_file = st.file_uploader("Sube TXT", type=["txt"], key="txt_flow")

    if not pdf_file or not txt_file:
        st.info("Carga ambos archivos para procesar (PDF y TXT).")
        return

    try:
        pdf_bytes = pdf_file.read()
        txt_bytes = txt_file.read()
        registros = extract_registros_from_pdf_bytes(pdf_bytes)
        if not registros:
            st.warning("No se encontraron patrones 'Registro N' en el PDF.")
            return
        indices = sorted({r * MULTIPLICADOR for r in registros})
        txt_lines = read_txt_lines(txt_bytes)
        rows = build_rows_from_txt_indices(indices, txt_lines, codigo, descripcion)
        df = pd.DataFrame(rows, columns=OUT_COLUMNS)
        df["importe"] = pd.to_numeric(df["importe"], errors="coerce").fillna(0.0)

        st.subheader("Vista previa")
        st.dataframe(df.head(100))
        st.markdown(f"**Total importe:** {df['importe'].sum():,.2f}")

        excel_bytes = df_to_excel_bytes(df)
        st.download_button("Descargar Excel (PDF→TXT)", data=excel_bytes,
                           file_name="rechazos_desde_pdf_txt.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        logger.exception("Error in PDF->TXT flow")
        st.error(f"Ocurrió un error: {e}")

def zip_excel_tab(default_start_row: int = 12) -> None:
    st.header("Flujo ZIP → Excel")
    st.write("Sube un ZIP con un Excel. Procesa desde fila (1-based):")
    start_row = st.number_input("Fila inicial (1-based)", min_value=1, value=default_start_row, step=1)
    uploaded = st.file_uploader("Sube ZIP con Excel", type=["zip"], key="zip_flow")
    if not uploaded:
        st.info("Carga un ZIP que contenga al menos un archivo .xlsx o .xls.")
        return
    try:
        zip_bytes = uploaded.read()
        df_excel = read_first_excel_from_zip_bytes(zip_bytes)
        st.success(f"Excel cargado: {df_excel.shape[0]} filas, {df_excel.shape[1]} columnas")
        rows = build_rows_from_excel_df(df_excel, start_row=start_row)
        if not rows:
            st.warning("No se encontraron filas válidas desde la fila indicada con contenido en la columna O.")
        df_out = pd.DataFrame(rows, columns=OUT_COLUMNS)
        df_out["importe"] = pd.to_numeric(df_out["importe"], errors="coerce").fillna(0.0)

        st.subheader("Vista previa")
        st.dataframe(df_out.head(100))
        st.markdown(f"**Total importe:** {df_out['importe'].sum():,.2f}")

        excel_bytes = df_to_excel_bytes(df_out)
        st.download_button("Descargar Excel (ZIP→Excel)", data=excel_bytes,
                           file_name="rechazos_desde_zip.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        logger.exception("Error in ZIP->Excel flow")
        st.error(f"Ocurrió un error al procesar el ZIP/Excel: {e}")

def main() -> None:
    st.set_page_config(layout="centered", page_title="Rechazos MASIVOS")
    st.title("RECHAZOS DE PAGOS MASIVOS — UNIFICADO")
    tab1, tab2 = st.tabs(["PDF → TXT", "ZIP → Excel"])
    with tab1:
        pdf_txt_tab()
    with tab2:
        zip_excel_tab(default_start_row=12)
    st.caption("Ajusta posiciones o fila inicial si tu archivo tiene encabezados o desplazamientos diferentes.")

if __name__ == "__main__":
    main()
