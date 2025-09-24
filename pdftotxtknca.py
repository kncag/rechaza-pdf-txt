# streamlit_app.py
from __future__ import annotations
import io
import re
import zipfile
import logging
from typing import List, Dict, Tuple, Optional

import streamlit as st
import pandas as pd

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

# -----------------------
# Configuración
# -----------------------
TXT_POS = {
    "dni": (25, 33),
    "nombre": (40, 85),
    "referencia": (115, 126),
    "importe": (186, 195),
}

ESTADO_FIJO = "rechazada"
MULTIPLICADOR = 2  # fijo para PDF->TXT

PDF_RECHAZOS: Dict[str, Tuple[str, str]] = {
    "R002: CUENTA INVALIDA": ("R002", "CUENTA INVALIDA"),
    "R001: DOCUMENTO ERRADO": ("R001", "DOCUMENTO ERRADO"),
}

RECHAZO_R016 = ("R016", "CLIENTE NO TITULAR DE LA CUENTA")
RECHAZO_R002 = ("R002", "CUENTA INVALIDA")
KEYWORDS_NO_TITULAR = [
    "no es titular", "beneficiario no", "cliente no titular", "no titular",
    "continuar", "puedes continuar", "si deseas", "si deseas, puedes continuar"
]

OUT_COLUMNS = [
    "dni/cex",
    "nombre",
    "importe",
    "Referencia",
    "Estado",
    "Codigo de Rechazo",
    "Descripcion de Rechazo",
]

# logging
logger = logging.getLogger("rechazos_app")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# -----------------------
# Utilidades
# -----------------------
def parse_amount(raw: Optional[str]) -> float:
    if raw is None:
        return 0.0
    s = str(raw).strip()
    s = re.sub(r"[^\d,.-]", "", s)
    if not s:
        return 0.0
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(s)
    except ValueError:
        return 0.0

def slice_fixed(line: str, start_1: int, end_1: int) -> str:
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
# PDF->TXT
# -----------------------
def extract_registros_from_pdf_bytes(pdf_bytes: bytes) -> List[int]:
    if fitz is None:
        raise RuntimeError("PyMuPDF (fitz) not available")
    text_parts = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            text_parts.append(page.get_text() or "")
    joined = "\n".join(text_parts)
    matches = re.findall(r"Registro\s+(\d{1,5})", joined, re.IGNORECASE)
    nums = sorted({int(m) for m in matches})
    logger.info("Registros encontrados: %s", nums)
    return nums

def read_txt_lines(txt_bytes: bytes, encoding: str = "utf-8") -> List[str]:
    s = txt_bytes.decode(encoding, errors="replace")
    return [ln.rstrip("\r\n") for ln in s.splitlines()]

def build_rows_from_txt_indices(indices: List[int], txt_lines: List[str], codigo: str, descripcion: str) -> List[Dict]:
    rows = []
    total = len(txt_lines)
    for idx in indices:
        if idx < 1 or idx > total:
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
# ZIP->Excel
# -----------------------
def read_first_excel_from_zip_bytes(zip_bytes: bytes) -> pd.DataFrame:
    buf = io.BytesIO(zip_bytes)
    with zipfile.ZipFile(buf) as z:
        candidates = [n for n in z.namelist() if n.lower().endswith((".xlsx", ".xls"))]
        if not candidates:
            raise ValueError("No Excel found in ZIP")
        first = candidates[0]
        with z.open(first) as f:
            df = pd.read_excel(f, sheet_name=0, dtype=str)
    logger.info("Excel cargado desde ZIP: %s filas x %s cols", df.shape[0], df.shape[1])
    return df

def get_col_by_letter_from_row(row: pd.Series, letter: str) -> str:
    idx = ord(letter.upper()) - ord("A")
    vals = list(row.values)
    if idx < 0 or idx >= len(vals):
        return ""
    val = vals[idx]
    return "" if pd.isna(val) else str(val).strip()

def build_rows_from_excel_df(df: pd.DataFrame, start_row: int = 12) -> List[Dict]:
    rows = []
    if df.shape[0] < start_row:
        return rows
    df_proc = df.iloc[start_row - 1 :].reset_index(drop=True)
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
    logger.info("Filas construidas desde Excel: %d", len(rows))
    return rows

# -----------------------
# UI modular
# -----------------------
def pdf_txt_tab() -> None:
    st.header("Flujo PDF → TXT")
    st.write("Sube PDF y TXT. Extrae 'Registro N' del PDF, multiplica por 2 internamente, busca la línea en el TXT y genera el Excel.")
    # Mostrar opción actual y selectbox sólo con las otras opciones
    default_key = list(PDF_RECHAZOS.keys())[0]
    selected_label_display = st.session_state.get("pdf_rechazo_selected", default_key)
    st.write("Código de Rechazo actual:", f"**{selected_label_display}**")
    # opciones de cambio: excluye la actualmente mostrada
    other_opts = [k for k in PDF_RECHAZOS.keys() if k != selected_label_display]
    change = st.selectbox("Cambiar a (opcional)", ["Mantener actual"] + other_opts, index=0)
    if change != "Mantener actual":
        selected_label = change
        st.session_state["pdf_rechazo_selected"] = change
    else:
        selected_label = selected_label_display
    codigo, descripcion = PDF_RECHAZOS[selected_label]

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
        logger.exception("Error en PDF->TXT")
        st.error(f"Ocurrió un error: {e}")

def zip_excel_tab() -> None:
    st.header("Flujo ZIP → Excel")
    st.write("Sube un ZIP con un Excel. Se procesará desde la fila 12 (fija) y sólo filas con contenido en la columna O serán incluidas.")
    uploaded = st.file_uploader("Sube ZIP con Excel", type=["zip"], key="zip_flow")
    if not uploaded:
        st.info("Carga un ZIP que contenga al menos un archivo .xlsx o .xls.")
        return
    try:
        zip_bytes = uploaded.read()
        df_excel = read_first_excel_from_zip_bytes(zip_bytes)
        st.success(f"Excel cargado: {df_excel.shape[0]} filas, {df_excel.shape[1]} columnas")
        rows = build_rows_from_excel_df(df_excel, start_row=12)  # fija en 12
        if not rows:
            st.warning("No se encontraron filas válidas desde la fila 12 con contenido en la columna O.")
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
        logger.exception("Error en ZIP->Excel")
        st.error(f"Ocurrió un error al procesar el ZIP/Excel: {e}")

def main() -> None:
    st.set_page_config(layout="centered", page_title="Rechazos MASIVOS")
    # ancho opcional
    st.markdown(
        """
        <style>
            .main > div.block-container { max-width: 900px; padding-left: 1rem; padding-right: 1rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("RECHAZOS DE PAGOS MASIVOS — UNIFICADO")
    tab1, tab2 = st.tabs(["PDF → TXT", "ZIP → Excel"])
    with tab1:
        pdf_txt_tab()
    with tab2:
        zip_excel_tab()
    st.caption("Ajusta posiciones o fila inicial si tu archivo tiene encabezados o desplazamientos diferentes.")

if __name__ == "__main__":
    main()
