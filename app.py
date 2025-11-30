import streamlit as st
import pandas as pd
from io import BytesIO
import logic_processor as logic

st.set_page_config(page_title="Robot Conciliación (Drag & Drop)", layout="wide")
st.title("📂 Carga Masiva (Lógica Original)")

st.info("Arrastra tus archivos `.txt`. Se subirán con su nombre original usando la lógica de reintentos avanzada.")

uploaded_files = st.file_uploader("Archivos", accept_multiple_files=True, type=['txt'])

if uploaded_files and st.button("🚀 INICIAR", type="primary"):
    
    audit_rows = []
    bar = st.progress(0)
    status_box = st.empty()
    total = len(uploaded_files)
    
    for i, file in enumerate(uploaded_files):
        original_name = file.name
        content_bytes = file.getvalue()
        try:
            content_str = content_bytes.decode('utf-8', errors='ignore')
        except:
            content_str = ""
            
        status_box.text(f"Procesando: {original_name}")
        
        # 1. CLASIFICACIÓN
        sys_name, sub_id, flow_key = logic.detect_system_and_subscription(original_name)
        
        if not sub_id:
            audit_rows.append({
                "Archivo": original_name, "Estado": "🚫 Ignorado", 
                "Detalles": "No coincide con reglas EURO/UDEP", "Procesados": 0, "Reconciliados": 0
            })
            bar.progress((i+1)/total)
            continue
            
        # 2. VALIDACIÓN (Solo para alertar o saltar vacíos críticos)
        es_valido, razon, lineas = logic.validar_contenido(original_name, content_str)
        if not es_valido:
            audit_rows.append({
                "Archivo": original_name, "Estado": "🚫 Omitido", 
                "Detalles": razon, "Procesados": 0, "Reconciliados": 0
            })
            bar.progress((i+1)/total)
            continue
        
        # 3. SUBIDA API (Usando nombre ORIGINAL)
        res = logic.api_upload_flow(content_bytes, original_name, sub_id, flow_key, lineas)
        
        audit_rows.append({
            "Archivo": original_name,
            "Sistema": sys_name,
            "Estado": res['status'],
            "Procesados": res['proc'],
            "Reconciliados": res['rec'],
            "Detalles": res['details']
        })
        
        bar.progress((i+1)/total)

    status_box.success("Proceso Finalizado")
    
    if audit_rows:
        df = pd.DataFrame(audit_rows)
        st.dataframe(df, use_container_width=True)
        
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        st.download_button("Descargar Auditoría", buffer.getvalue(), "auditoria.xlsx")
