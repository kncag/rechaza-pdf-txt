# streamlit_app.py
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

st.title("RECHAZOS DE PAGOS MASIVOS — Extraer Registros y Generar Excel")
st.divider()
st.write("Sube un PDF (contiene 'Registro N') y un TXT (línea 1 = registro 1). La app duplicará N, leerá esa línea del TXT y generará un Excel descargable.")

col1, col2 = st.columns(2)
with col1:
    pdf_file = st.file_uploader("Sube PDF", type="pdf")
with col2:
    txt_file = st.file_uploader("Sube TXT", type=["txt"])

factor = st.number_input("Factor multiplicador", value=2, min_value=1, step=1)

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

def build_rows_from_indices(line_indices, txt_lines):
    rows = []
    total = len(txt_lines)
    for idx in line_indices:
        if idx < 1 or idx > total:
            dni = nombre = referencia = importe = ""
        else:
            line = txt_lines[idx - 1]
            dni = slice_column_by_1based(line, COL_DNI_START, COL_DNI_END)
            nombre = slice_column_by_1based(line, COL_NOMBRE_START, COL_NOMBRE_END)
            referencia = slice_column_by_1based(line, COL_REFERENCIA_START, COL_REFERENCIA_END)
            importe = slice_column_by_1based(line, COL_IMPORTE_START, COL_IMPORTE_END)
        rows.append({
            "dni/cex": dni,
            "nombre": nombre,
            "importe": importe,
            "REFERENCIA": referencia,
            "codigo rechazo": CODIGO_RECHAZO,
            "rechazo": RECHAZO_TXT
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
            st.write("Registros encontrados (PDF):", registros)
            multiplied = sorted({r * int(factor) for r in registros})
            st.write(f"Después de multiplicar por {factor}:", multiplied)

            txt_lines = read_txt_lines_from_bytes(txt_bytes)
            rows = build_rows_from_indices(multiplied, txt_lines)

            # Vista previa en DataFrame (limitado)
            df = pd.DataFrame(rows, columns=["dni/cex", "nombre", "importe", "REFERENCIA", "codigo rechazo", "rechazo"])
            st.subheader("Vista previa (primeras 50 filas)")
            st.dataframe(df.head(50))

            # Guardar Excel en memoria y ofrecer descarga
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
