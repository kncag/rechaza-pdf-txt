# streamlit_app_unified_final.py
import streamlit as st
import fitz              # pip install PyMuPDF
import re
import io
import pandas as pd      # pip install pandas openpyxl
import zipfile

# --- Constantes globales ---
# Posiciones 1-based (flujo PDF->TXT)
COL_DNI_START, COL_DNI_END = 25, 33
COL_NOMBRE_START, COL_NOMBRE_END = 40, 85
COL_REFERENCIA_START, COL_REFERENCIA_END = 115, 126
COL_IMPORTE_START, COL_IMPORTE_END = 186, 195

ESTADO_FIJO = "rechazada"
MULTIPLICADOR = 2  # fijo para el flujo PDF->TXT

# Rechazos para selector del flujo PDF->TXT
RECHAZO_OPCIONES = {
    "R002: CUENTA INVALIDA": ("R002", "CUENTA INVALIDA"),
    "R001: DOCUMENTO ERRADO": ("R001", "DOCUMENTO ERRADO")
}

# Rechazos para flujo ZIP->Excel basado en Col O
RECHAZO_R016 = ("R016", "CLIENTE NO TITULAR DE LA CUENTA")
RECHAZO_R002 = ("R002", "CUENTA INVALIDA")
KEYWORDS_NO_TITULAR = [
    "no es titular", "beneficiario no", "cliente no titular", "no titular",
    "continuar", "puedes continuar", "si deseas, puedes continuar", "continuar con"
]

# --- Helpers comunes ---
def parse_importe_to_float(raw):
    if raw is None:
        return 0.0
    s = str(raw).strip()
    s = re.sub(r'[^\d,.-]', '', s)
    if s == "":
        return 0.0
    if '.' in s and ',' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        if ',' in s and '.' not in s:
            s = s.replace(',', '.')
    if s.count('.') > 1:
        parts = s.split('.')
        s = ''.join(parts[:-1]) + '.' + parts[-1]
    try:
        return float(s)
    except ValueError:
        return 0.0

def slice_column_by_1based(line, start_1, end_1):
    start_idx = max(0, start_1 - 1)
    if start_idx >= len(line):
        return ""
    return line[start_idx:end_1].strip()

# --- FLUJO A: PDF -> TXT -> Excel ---
def extract_registros_from_pdf_bytes(pdf_bytes):
    text = ""
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            text += page.get_text()
    matches = re.findall(r'Registro\s+(\d{1,5})', text, re.IGNORECASE)
    nums = sorted({int(m) for m in matches})
    return nums

def read_txt_lines_from_bytes(txt_bytes):
    s = txt_bytes.decode("utf-8", errors="replace")
    return [ln.rstrip("\n\r") for ln in s.splitlines()]

def build_rows_from_indices_pdf(indices, txt_lines, codigo_rechazo, descripcion_rechazo):
    rows = []
    total = len(txt_lines)
    for idx in indices:
        if idx < 1 or idx > total:
            dni = nombre = referencia = ""
            importe_val = 0.0
        else:
            line = txt_lines[idx - 1]
            dni = slice_column_by_1based(line, COL_DNI_START, COL_DNI_END)
            nombre = slice_column_by_1based(line, COL_NOMBRE_START, COL_NOMBRE_END)
            referencia = slice_column_by_1based(line, COL_REFERENCIA_START, COL_REFERENCIA_END)
            raw_importe = slice_column_by_1based(line, COL_IMPORTE_START, COL_IMPORTE_END)
            importe_val = parse_importe_to_float(raw_importe)
        rows.append({
            "dni/cex": dni,
            "nombre": nombre,
            "importe": importe_val,
            "Referencia": referencia,
            "Estado": ESTADO_FIJO,
            "Codigo de Rechazo": codigo_rechazo,
            "Descripcion de Rechazo": descripcion_rechazo
        })
    return rows

# --- FLUJO B: ZIP -> Excel -> transformar columnas por letra ---
def contains_no_titular(text):
    if not isinstance(text, str):
        return False
    t = text.lower()
    for k in KEYWORDS_NO_TITULAR:
        if k in t:
            return True
    return False

def read_first_excel_from_zip(zip_bytes):
    buf = io.BytesIO(zip_bytes)
    with zipfile.ZipFile(buf) as z:
        candidates = [n for n in z.namelist() if n.lower().endswith((".xlsx", ".xls"))]
        if not candidates:
            raise ValueError("No se encontró ningún archivo Excel dentro del ZIP.")
        first = candidates[0]
        with z.open(first) as f:
            df = pd.read_excel(f, sheet_name=0, dtype=str)
    return df

def get_col_by_letter_from_row(row, letter):
    idx = ord(letter.upper()) - ord('A')
    vals = list(row.values)
    if idx >= len(vals) or idx < 0:
        return ""
    val = vals[idx]
    return "" if pd.isna(val) else str(val).strip()

def build_rows_from_excel_df(df):
    """
    Procesa el DataFrame comenzando en la fila 13 (índice 12).
    Sólo incluye filas cuya columna O contenga información (no vacía después de strip).
    """
    rows = []
    # Si hay menos de 13 filas, no hay datos a procesar
    if df.shape[0] <= 12:
        return rows
    df_proc = df.iloc[12:].reset_index(drop=True)  # ahora index 0 == fila original 13

    for _, row in df_proc.iterrows():
        columna_O = get_col_by_letter_from_row(row, 'O')
        if not columna_O:
            continue  # saltar filas sin contenido en columna O

        dni = get_col_by_letter_from_row(row, 'E')
        nombre = get_col_by_letter_from_row(row, 'F')
        importe_raw = get_col_by_letter_from_row(row, 'N')
        referencia = get_col_by_letter_from_row(row, 'H')

        importe = parse_importe_to_float(importe_raw)
        if contains_no_titular(columna_O):
            codigo, descripcion = RECHAZO_R016
        else:
            codigo, descripcion = RECHAZO_R002

        rows.append({
            "dni/cex": dni,
            "nombre": nombre,
            "importe": importe,
            "Referencia": referencia,
            "Estado": ESTADO_FIJO,
            "Codigo de Rechazo": codigo,
            "Descripcion de Rechazo": descripcion
        })
    return rows

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("RECHAZOS DE PAGOS MASIVOS — UNIFICADO")
tabs = st.tabs(["PDF → TXT", "ZIP → Excel"])

# PESTAÑA 1: PDF -> TXT
with tabs[0]:
    st.subheader("Flujo PDF → TXT")
    st.write("Sube PDF y TXT. Extrae 'Registro N' del PDF, multiplica por 2 (interno), busca la línea en el TXT y genera el Excel.")
    selected = st.selectbox("Elige Código de Rechazo (para este flujo)", list(RECHAZO_OPCIONES.keys()))
    CODIGO_RECHAZO, RECHAZO_TXT = RECHAZO_OPCIONES[selected]

    col1, col2 = st.columns(2)
    with col1:
        pdf_file = st.file_uploader("Sube PDF", type="pdf", key="pdf_flow")
    with col2:
        txt_file = st.file_uploader("Sube TXT", type=["txt"], key="txt_flow")

    if pdf_file and txt_file:
        try:
            pdf_bytes = pdf_file.read()
            txt_bytes = txt_file.read()
            registros = extract_registros_from_pdf_bytes(pdf_bytes)
            if not registros:
                st.warning("No se encontraron patrones 'Registro N' en el PDF.")
            else:
                # multiplicar internamente por 2 (sin exponer)
                indices = sorted({r * MULTIPLICADOR for r in registros})
                txt_lines = read_txt_lines_from_bytes(txt_bytes)
                rows = build_rows_from_indices_pdf(indices, txt_lines, CODIGO_RECHAZO, RECHAZO_TXT)

                df = pd.DataFrame(rows, columns=[
                    "dni/cex", "nombre", "importe", "Referencia", "Estado", "Codigo de Rechazo", "Descripcion de Rechazo"
                ])
                df["importe"] = pd.to_numeric(df["importe"], errors="coerce").fillna(0.0)

                st.subheader("Vista previa (primeras 50 filas)")
                st.dataframe(df.head(50))
                st.markdown(f"**Suma de importe detectada:** {df['importe'].sum():,.2f}")

                output = io.BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False, sheet_name="Rechazos")
                output.seek(0)

                st.download_button(
                    label="Descargar Excel (PDF→TXT)",
                    data=output.getvalue(),
                    file_name="rechazos_desde_pdf_txt.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        except Exception as e:
            st.error(f"Ocurrió un error en el flujo PDF→TXT: {e}")

# PESTAÑA 2: ZIP -> Excel
with tabs[1]:
    st.subheader("Flujo ZIP → Excel")
    st.write("Sube un ZIP que contenga un archivo Excel. Procesa desde fila 13 y sólo filas con contenido en columna O.")
    zip_file = st.file_uploader("Sube ZIP con Excel", type=["zip"], key="zip_flow")
    if zip_file is not None:
        try:
            zip_bytes = zip_file.read()
            df_excel = read_first_excel_from_zip(zip_bytes)
            st.success(f"Excel cargado: {df_excel.shape[0]} filas, {df_excel.shape[1]} columnas (primer archivo en el ZIP).")

            rows = build_rows_from_excel_df(df_excel)
            if not rows:
                st.warning("No se encontraron filas válidas desde la fila 13 con contenido en la columna O.")
            df_out = pd.DataFrame(rows, columns=[
                "dni/cex", "nombre", "importe", "Referencia", "Estado", "Codigo de Rechazo", "Descripcion de Rechazo"
            ])
            df_out["importe"] = pd.to_numeric(df_out["importe"], errors="coerce").fillna(0.0)

            st.subheader("Vista previa (primeras 50 filas)")
            st.dataframe(df_out.head(50))
            st.markdown(f"**Suma total importe:** {df_out['importe'].sum():,.2f}")

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_out.to_excel(writer, index=False, sheet_name="Rechazos")
            output.seek(0)

            st.download_button(
                label="Descargar Excel (ZIP→Excel)",
                data=output.getvalue(),
                file_name="rechazos_desde_zip.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Ocurrió un error en el flujo ZIP→Excel: {e}")

# Footer
st.caption("Ajusta las posiciones de columnas si tu TXT o Excel tienen desplazamientos. Si quieres elegir entre varios xlsx dentro del ZIP, lo agrego.")
