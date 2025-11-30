import streamlit as st
import pandas as pd
from io import BytesIO
import logic_processor as logic

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Robot Conciliación Pro", page_icon="⚡", layout="wide")

st.markdown("""
<style>
    .stProgress > div > div > div > div { background-color: #00cc00; }
    .log-box { background-color: #f0f2f6; padding: 10px; border-radius: 5px; margin-bottom: 10px; font-family: monospace; }
</style>
""", unsafe_allow_html=True)

st.title("⚡ Conciliación Masiva Inteligente")
st.markdown("### 📥 Arrastra tus archivos")

# --- COLUMNAS DE CARGA ---
col1, col2 = st.columns(2)
with col1:
    st.info("🚗 **ZONA EURO MOTORS** (REC/EURO)")
    files_euro = st.file_uploader("Archivos EURO", accept_multiple_files=True, type=['txt'], key="euro")
with col2:
    st.success("🎓 **ZONA UDEP** (REC/UDEP)")
    files_udep = st.file_uploader("Archivos UDEP", accept_multiple_files=True, type=['txt'], key="udep")

# --- UNIFICAR ARCHIVOS ---
all_files = (files_euro or []) + (files_udep or [])

if all_files and st.button("🚀 INICIAR PROCESAMIENTO", type="primary", use_container_width=True):
    
    # 1. Preparar cola de trabajo
    queue = []
    if files_euro:
        for f in files_euro: queue.append((f, "EURO", logic.RULES_EURO, "euro"))
    if files_udep:
        for f in files_udep: queue.append((f, "UDEP", logic.RULES_UDEP, "udep"))
        
    total_files = len(queue)
    audit_rows = []
    progress_bar = st.progress(0)
    
    # Contenedor principal para los logs
    logs_container = st.container()

    for i, (file, sys_name, rules, flow_key) in enumerate(queue):
        fname = file.name
        
        # --- PASO CRÍTICO: REBOBINAR Y LEER BYTES ---
        file.seek(0)
        content_bytes = file.read()
        file_size = len(content_bytes)
        
        try: content_str = content_bytes.decode('utf-8', errors='ignore')
        except: content_str = ""
        
        # --- LÓGICA ---
        sub_id = logic.find_subscription_id(fname, rules)
        
        # --- VISUALIZACIÓN DEL LOG (SIEMPRE VISIBLE) ---
        with logs_container:
            st.divider()
            
            # Mostramos Nombre y CÓDIGO YK
            if sub_id:
                st.markdown(f"#### 📄 {fname} | 🔑 `{sub_id}`")
            else:
                st.markdown(f"#### 📄 {fname} | ❌ `SIN CÓDIGO`")

            # Procesamiento
            if not sub_id:
                st.error(f"Ignorado: No coincide con reglas de {sys_name}")
                audit_rows.append({"Archivo": fname, "Sistema": sys_name, "Estado": "🚫 Error Regla", 
                                   "Detalles": "Nombre no reconocido", "Proc": 0, "Rec": 0})
            else:
                valido, razon, lineas = logic.validar_contenido(fname, content_str)
                if not valido:
                    st.warning(f"Omitido: {razon}")
                    audit_rows.append({"Archivo": fname, "Sistema": sys_name, "Estado": "⚠️ Omitido", 
                                       "Detalles": razon, "Proc": 0, "Rec": 0})
                else:
                    # Llamada a API
                    with st.spinner(f"Subiendo a {sys_name} ({lineas} líneas)..."):
                        res = logic.api_upload_flow(content_bytes, fname, sub_id, flow_key, lineas)
                    
                    # Mostrar Consola de Logs
                    log_text = "\n".join(res['logs'])
                    st.code(log_text, language="text")
                    
                    # Resultado Final
                    msg = f"Resultado: {res['status']} (Rec: {res['rec']})"
                    if "Exitoso" in res['status']: st.success(msg)
                    elif "Sin Datos" in res['status']: st.info(msg)
                    else: st.warning(msg)

                    audit_rows.append({
                        "Archivo": fname, "Sistema": sys_name, "Estado": res['status'],
                        "Detalles": res['details'], "Proc": res['proc'], "Rec": res['rec']
                    })
        
        progress_bar.progress((i + 1) / total_files)

    st.success("✅ ¡Proceso finalizado!")

    # --- TABLA Y DESCARGA ---
    if audit_rows:
        df = pd.DataFrame(audit_rows)
        st.subheader("📊 Resumen Final")
        
        # Métricas
        c1, c2, c3 = st.columns(3)
        c1.metric("Archivos", len(df))
        c2.metric("Procesados OK", df[df['Estado'].astype(str).str.contains("Exitoso")].shape[0])
        c3.metric("Reconciliados Total", df['Rec'].sum())

        def color_row(val):
            s = str(val)
            color = 'black'
            if 'Exitoso' in s: color = '#28a745'
            elif 'Fallos' in s: color = '#ffc107'
            elif 'Error' in s: color = '#dc3545'
            elif 'Sin Datos' in s: color = '#17a2b8'
            return f'color: {color}; font-weight: bold'

        st.dataframe(df.style.map(color_row, subset=['Estado']), use_container_width=True)
        
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
            
        st.download_button("📥 Descargar Excel", buffer.getvalue(), "auditoria_final.xlsx", type="primary")
