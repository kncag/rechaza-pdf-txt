# streamlit_app_final.py
import streamlit as st
import fitz              # pip install PyMuPDF
import re
import io
import pandas as pd      # pip install pandas openpyxl

# --- Configuración de posiciones (1-based, inclusivas) ---
COL_DNI_START, COL_DNI_END = 25, 33
COL_NOMBRE_START, COL_NOMBRE_END = 40, 85
COL_REFERENCIA_START, COL_REFERENCIA_END = 115, 126
COL_IMPORTE_START, COL_IMPORTE_END = 186, 195

CODIGO_RECHAZO = "R001"
RECHAZO_TXT = "DOCUMENTO ERRADO"
ESTADO_FIJO = "rechazada"
MULTIPLICADOR = 2  # fijo en 2, no expuesto en la UI

st.title("RECHAZOS DE PAGOS MASIVOS — Extraer Registros y Generar Excel")
st.divider()
st.write("Sube un PDF y un TXT. La app extrae 'Registro N', multiplica N por 2, lee esa línea del TXT y genera un Excel descargable.")

col1, col2 = st.columns(2)
with col1:
    pdf_file = st.file_uploader("Sube PDF", type="pdf")
with col2:
    txt_file = st.file_uploader("Sube TXT", type=["txt"])

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

def slice_column_by_1based(line, start_1, end_1):
    start_idx = max(0, start_1 - 1)
    if start_idx >= len(line):
        return ""
    return line[start_idx:end_1].strip()

def parse_importe_to_float(raw):
    if not raw:
        return 0.0
    s = raw.strip()
    s = re.sub(r'[^\d,.-]', '', s)
    if s == "":
        return 0.0
    if '.' in s and ',' in s:
        s = s.replace('.', '')
        s = s.replace(',', '.')
    else:
        if ',' in s and '.' not in s:
            s = s.replace(',', '.')
    if s.count('.') > 1:
        parts = s.split('.')
        integer = ''.join(parts[:-1])
        decimal = parts[-1]
        s = integer + '.' + decimal
    try:
        return float(s)
    except ValueError:
        return 0.0

def build_rows_from_indices(line_indices, txt_lines):
    rows = []
    total = len(txt_lines)
    for idx in line_indices:
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
            "Codigo de Rechazo": CODIGO_RECHAZO,
            "Descripcion de Rechazo": RECHAZO_TXT
        })
    return rows

if pdf_file and txt_file:
    try:
        pdf_bytes = pdf_file.read()
        txt_bytes = txt_file.read()

        registros = extract_registros_from_pdf_bytes(pdf_bytes)
        if not registros:
            st.warning("No se encontraron patrones 'Registro N' en el PDF.")
        else:
            # Multiplicación fija por 2 (sin mostrar listas intermedias)
            indices = sorted({r * MULTIPLICADOR for r in registros})

            txt_lines = read_txt_lines_from_bytes(txt_bytes)
            rows = build_rows_from_indices(indices, txt_lines)

            df = pd.DataFrame(rows, columns=[
                "dni/cex", "nombre", "importe", "Referencia", "Estado", "Codigo de Rechazo", "Descripcion de Rechazo"
            ])
            df["importe"] = pd.to_numeric(df["importe"], errors="coerce").fillna(0.0)

            st.subheader("Vista previa (primeras 50 filas)")
            st.dataframe(df.head(50))

            total_importe = df["importe"].sum()
            st.markdown(f"**Suma de importe detectada:** {total_importe:,.2f}")

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Rechazos")
            output.seek(0)

            st.download_button(
                label="Descargar Excel con resultados",
                data=output.getvalue(),
                file_name="rechazos_generados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"Ocurrió un error: {e}")
else:
    st.info("Carga ambos archivos para procesar (PDF y TXT).")
