import streamlit as st
import pandas as pd
from io import BytesIO
import logic_processor as logic

# Configuración visual
st.set_page_config(page_title="Carga Masiva API", page_icon="📂", layout="wide")

st.title("📂 Carga Masiva a API (Drag & Drop)")
st.markdown("""
Sube tus archivos `.txt` (puedes mezclar EURO y UDEP). 
El sistema detectará automáticamente a dónde pertenece cada uno.
""")

# --- ZONA DE CARGA ---
uploaded_files = st.file_uploader(
    "Arrastra tus archivos aquí", 
    accept_multiple_files=True, 
    type=['txt']
)

# --- BOTÓN DE ACCIÓN ---
if uploaded_files and st.button("🚀 PROCESAR ARCHIVOS", type="primary"):
    
    audit_rows = []
    total_files = len(uploaded_files)
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    success_count = 0
    
    for i, uploaded_file in enumerate(uploaded_files):
        fname = uploaded_file.name
        fcontent = uploaded_file.getvalue()
        
        status_text.text(f"Analizando: {fname}...")
        
        # 1. DETECCIÓN AUTOMÁTICA
        system_name, sub_id, flow_key = logic.detect_system_and_subscription(fname)
        
        if sub_id:
            # 2. SUBIDA A API
            status_text.text(f"Subiendo {fname} a {system_name}...")
            res = logic.api_upload_flow(fcontent, fname, sub_id, flow_key)
            
            audit_rows.append({
                "Archivo": fname,
                "Sistema": system_name,
                "Subscripción": sub_id,
                "Estado": res['status'],
                "Procesados": res['processed'],
                "Reconciliados": res['reconciled'],
                "Detalles": res['details']
            })
            success_count += 1
        else:
            # 3. NO SE RECONOCE EL ARCHIVO
            audit_rows.append({
                "Archivo": fname,
                "Sistema": "DESCONOCIDO",
                "Subscripción": "-",
                "Estado": "🚫 Omitido",
                "Procesados": 0,
                "Reconciliados": 0,
                "Detalles": "Nombre no coincide con reglas EURO ni UDEP"
            })
            
        # Actualizar barra
        progress_bar.progress((i + 1) / total_files)

    status_text.text("¡Proceso finalizado!")
    progress_bar.empty()
    
    # --- RESULTADOS ---
    st.divider()
    st.subheader("📊 Resultados")
    
    if audit_rows:
        df = pd.DataFrame(audit_rows)
        
        # Métricas
        c1, c2, c3 = st.columns(3)
        c1.metric("Archivos Totales", total_files)
        c2.metric("Detectados y Subidos", success_count)
        c3.metric("Registros Procesados", df['Procesados'].sum())
        
        # Colorear la tabla según estado
        def color_status(val):
            color = 'black'
            if 'Exitosamente' in val: color = 'green'
            elif 'Fallos' in val: color = 'orange'
            elif 'Omitido' in val: color = 'red'
            elif 'Sin Datos' in val: color = 'blue'
            return f'color: {color}; font-weight: bold'

        st.dataframe(df.style.map(color_status, subset=['Estado']), use_container_width=True)
        
        # Descarga Excel
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Auditoria')
            
        st.download_button(
            label="📥 Descargar Auditoría Excel",
            data=buffer.getvalue(),
            file_name="auditoria_carga_masiva.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("No se generaron resultados.")

elif not uploaded_files:
    st.info("Esperando archivos...")
