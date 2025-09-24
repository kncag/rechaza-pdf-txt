# streamlit_app_with_zip_flow.py
import streamlit as st
import fitz              # si ya lo usas en la app principal
import re
import io
import pandas as pd
import zipfile
from openpyxl import load_workbook

# --- Constantes y mapeos ---
ESTADO_FIJO = "rechazada"

# Rechazos alternativos
RECHAZO_R016 = ("R016", "CLIENTE NO TITULAR DE LA CUENTA")
RECHAZO_R002 = ("R002", "CUENTA INVALIDA")

# Palabras claves para detectar "no titular / continuar"
KEYWORDS_NO_TITULAR = [
    "no es titular", "beneficiario no", "cliente no titular", "no titular",
    "continuar", "puedes continuar", "si deseas, puedes continuar", "continuar con"
]

# Helper: extraer texto limpio y buscar coincidencias clave
def contains_no_titular(text):
    if not isinstance(text, str):
        return False
    t = text.lower()
    for k in KEYWORDS_NO_TITULAR:
        if k in t:
            return True
    return False

# Helper: leer primer xlsx dentro de un zip y devolver DataFrame (primera hoja)
def read_first_excel_from_zip(zip_bytes):
    buf = io.BytesIO(zip_bytes)
    with zipfile.ZipFile(buf) as z:
        # buscar primer archivo con extensión xlsx/xls
        candidates = [n for n in z.namelist() if n.lower().endswith((".xlsx", ".xls"))]
        if not candidates:
            raise ValueError("No se encontró ningún archivo Excel dentro del ZIP.")
        first = candidates[0]
        with z.open(first) as f:
            # pandas puede leer bytes de Excel directamente
            df = pd.read_excel(f, sheet_name=0, dtype=str)  # leer todo como str para evitar conversiones
    return df

# Helper: obtener valor por letra de columna (1-based)
def get_col_by_letter(df_row, letter):
    # convertir letra a índice 0-based
    idx = ord(letter.upper()) - ord('A')
    if idx < 0:
        return ""
    # si DataFrame tiene columnas con nombres, podemos intentar acceder por posición
    try:
        # Convertir row a list de valores en orden
        vals = list(df_row.values)
        return "" if idx >= len(vals) else ("" if pd.isna(vals[idx]) else str(vals[idx]).strip())
    except Exception:
        return ""

# Construir filas resultado desde df del excel extraído
def build_rows_from_excel_df(df):
    rows = []
    # iterar por cada fila del DataFrame
    for _, row in df.iterrows():
        dni = get_col_by_letter(row, 'E')   # col E
        nombre = get_col_by_letter(row, 'F') # col F
        importe_raw = get_col_by_letter(row, 'N') # col N
        referencia = get_col_by_letter(row, 'H') # col H
        columna_O = get_col_by_letter(row, 'O')  # col O para decidir rechazo

        # Normalizar importe (simple): quitar caracteres no numéricos y normalizar coma/punto
        importe = 0.0
        if importe_raw:
            s = re.sub(r'[^\d,.-]', '', importe_raw)
            if s:
                # heurística simple de separadores
                if '.' in s and ',' in s:
                    s = s.replace('.', '').replace(',', '.')
                elif ',' in s and '.' not in s:
                    s = s.replace(',', '.')
                # manejar múltiples puntos: conservar última como decimal
                if s.count('.') > 1:
                    parts = s.split('.')
                    s = ''.join(parts[:-1]) + '.' + parts[-1]
                try:
                    importe = float(s)
                except Exception:
                    importe = 0.0

        # decidir rechazo
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

# --- Streamlit UI: segunda pestaña ---
st.title("Flujo ZIP → Excel de Rechazos")

st.write("Sube un archivo ZIP que contenga un Excel. Se generará un Excel con las columnas: dni/cex, nombre, importe, Referencia, Estado, Codigo de Rechazo, Descripcion de Rechazo.")

zip_file = st.file_uploader("Sube ZIP con Excel", type=["zip"])
if zip_file is not None:
    try:
        zip_bytes = zip_file.read()
        df_excel = read_first_excel_from_zip(zip_bytes)
        st.success(f"Excel cargado: {df_excel.shape[0]} filas, {df_excel.shape[1]} columnas (se usará el primer archivo Excel dentro del ZIP).")

        rows = build_rows_from_excel_df(df_excel)
        df_out = pd.DataFrame(rows, columns=[
            "dni/cex", "nombre", "importe", "Referencia", "Estado", "Codigo de Rechazo", "Descripcion de Rechazo"
        ])
        df_out["importe"] = pd.to_numeric(df_out["importe"], errors="coerce").fillna(0.0)

        st.subheader("Vista previa (primeras 50 filas)")
        st.dataframe(df_out.head(50))

        st.markdown(f"**Suma total importe:** {df_out['importe'].sum():,.2f}")

        # Generar Excel en memoria
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_out.to_excel(writer, index=False, sheet_name="Rechazos")
        output.seek(0)

        st.download_button(
            label="Descargar Excel generado",
            data=output.getvalue(),
            file_name="rechazos_desde_zip.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"Ocurrió un error al procesar el ZIP/Excel: {e}")
else:
    st.info("Sube un ZIP que contenga al menos un archivo .xlsx o .xls.")
