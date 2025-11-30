import streamlit as st
import pandas as pd
from io import BytesIO
import logic_processor as logic

# --- CONFIGURACIÓN CENTRADA ---
st.set_page_config(page_title="Robot Conciliación", page_icon="⚡", layout="centered")

st.markdown("""
<style>
    .stProgress > div > div > div > div { background-color: #00cc00; }
    .log-text { font-family: monospace; font-size: 12px; color: #333; }
    h1 { text-align: center; }
</style>
""", unsafe_allow_html=True)

st.title("⚡ Carga Masiva")

# --- INTERFAZ COMPACTA ---
# Usamos pestañas para ahorrar espacio en layout centrado
tab1, tab2 = st.tabs(["🚗 EURO MOTORS", "🎓 UDEP"])

with tab1:
    files_euro = st.file_uploader("Archivos EURO", accept_multiple_files=True, type=['txt'], key="euro")
with tab2:
    files_udep = st.file_uploader("Archivos UDEP", accept_multiple_files=True, type=['txt'], key="udep")

# --- BOTÓN ---
all_files = (files_euro or []) + (files_udep or [])

if all_files and st.button("🚀 PROCESAR AHORA", type="primary", use_container_width=True):
    
    # Preparar cola
    queue = []
    if files_euro:
        for f in files_euro: queue.append((f, "EURO", logic.RULES_EURO, "euro"))
    if files_udep:
        for f in files_udep: queue.append((f, "UDEP", logic.RULES_UDEP, "udep"))
        
    total = len(queue)
    audit_rows = []
    progress_bar = st.progress(0)
    
    # Contenedor para logs limpios
    st.write("---")
    status_container = st.container()

    for i, (file, sys_name, rules, flow_key) in enumerate(queue):
        fname = file.name
        
        # Lectura
        file.seek(0)
        content_bytes = file.read()
        try: content_str = content_bytes.decode('utf-8', errors='ignore')
        except: content_str = ""
        
        # Lógica
        sub_id = logic.find_subscription_id(fname, rules)
        
        # LOG VISUAL SIMPLIFICADO
        with status_container:
            col_icon, col_text = st.columns([1, 10])
            
            if not sub_id:
                col_icon.error("🚫")
                col_text.markdown(f"**{fname}**: Ignorado (Regla)")
                audit_rows.append({"Archivo": fname, "Estado": "Ignorado", "Detalles": "No match", "P": 0, "R": 0})
            else:
                valido, razon, lineas = logic.validar_contenido(fname, content_str)
                if not valido:
                    col_icon.warning("⚠️")
                    col_text.markdown(f"**{fname}**: Omitido ({razon})")
                    audit_rows.append({"Archivo": fname, "Estado": "Omitido", "Detalles": razon, "P": 0, "R": 0})
                else:
                    # Ejecución API
                    with col_text:
                        with st.spinner(f"Subiendo {fname}..."):
                            res = logic.api_upload_flow(content_bytes, fname, sub_id, flow_key, lineas)
                    
                    # Resultado final
                    icon = "✅" if "Exitoso" in res['status'] else ("ℹ️" if "Sin Datos" in res['status'] else "⚠️")
                    col_icon.write(icon)
                    
                    # Mostrar logs importantes en una línea expandible
                    with col_text.expander(f"**{fname}**: {res['status']} (Reg: {res['proc']} | Rec: {res['rec']})"):
                        st.code("\n".join(res['logs']), language="text")

                    audit_rows.append({
                        "Archivo": fname, "Sistema": sys_name, "Estado": res['status'],
                        "Detalles": res['details'], "P": res['proc'], "R": res['rec']
                    })
        
        progress_bar.progress((i + 1) / total)

    st.success("¡Finalizado!")

    # --- RESUMEN ---
    if audit_rows:
        df = pd.DataFrame(audit_rows)
        st.divider()
        st.caption("Resumen Rápido")
        st.dataframe(df[["Archivo", "Estado", "P", "R"]], use_container_width=True)
        
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        st.download_button("Descargar Excel Completo", buffer.getvalue(), "reporte.xlsx")
