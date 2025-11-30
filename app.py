import streamlit as st
import pandas as pd
from io import BytesIO
import logic_processor as logic

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Robot Conciliación Pro", page_icon="⚡", layout="wide")

# Estilos CSS para mejorar visuales
st.markdown("""
<style>
    .big-font { font-size:20px !important; font-weight: bold; }
    .stProgress > div > div > div > div { background-color: #00cc00; }
</style>
""", unsafe_allow_html=True)

st.title("⚡ Conciliación Masiva Inteligente")
st.markdown("### 📥 Arrastra tus archivos a la zona correspondiente")

# --- COLUMNAS DE CARGA ---
col1, col2 = st.columns(2)

with col1:
    st.info("🚗 **ZONA EURO MOTORS** (REC/EURO)")
    files_euro = st.file_uploader("Archivos EURO", accept_multiple_files=True, type=['txt'], key="euro")

with col2:
    st.success("🎓 **ZONA UDEP** (REC/UDEP)")
    files_udep = st.file_uploader("Archivos UDEP", accept_multiple_files=True, type=['txt'], key="udep")

# --- BOTÓN DE PROCESO ---
all_files = (files_euro or []) + (files_udep or [])

if all_files and st.button("🚀 INICIAR PROCESAMIENTO", type="primary", use_container_width=True):
    
    # Preparar cola de trabajo
    queue = []
    if files_euro:
        for f in files_euro: queue.append((f, "EURO", logic.RULES_EURO, "euro"))
    if files_udep:
        for f in files_udep: queue.append((f, "UDEP", logic.RULES_UDEP, "udep"))
        
    total_files = len(queue)
    audit_rows = []
    
    # Componentes UI dinámicos
    progress_bar = st.progress(0)
    status_text = st.empty()
    logs_expander = st.status("📝 Log de ejecución en tiempo real", expanded=True)
    
    for i, (file, sys_name, rules, flow_key) in enumerate(queue):
        fname = file.name
        status_text.markdown(f"**Procesando ({i+1}/{total_files}):** `{fname}`")
        
        # Leer contenido
        content = file.getvalue()
        try: content_str = content.decode('utf-8', errors='ignore')
        except: content_str = ""
        
        # 1. Identificar Suscripción
        sub_id = logic.find_subscription_id(fname, rules)
        
        if not sub_id:
            logs_expander.write(f"🚫 **{fname}**: No coincide con reglas de {sys_name}")
            audit_rows.append({"Archivo": fname, "Sistema": sys_name, "Estado": "🚫 Error Regla", 
                               "Detalles": "Nombre no reconocido", "Proc": 0, "Rec": 0})
        else:
            # 2. Validar
            valido, razon, lineas = logic.validar_contenido(fname, content_str)
            if not valido:
                logs_expander.write(f"⚠️ **{fname}**: Omitido por validación ({razon})")
                audit_rows.append({"Archivo": fname, "Sistema": sys_name, "Estado": "⚠️ Omitido", 
                                   "Detalles": razon, "Proc": 0, "Rec": 0})
            else:
                # 3. Ejecutar API
                logs_expander.write(f"🔄 **{fname}**: Subiendo a {sys_name} ({lineas} líneas)...")
                res = logic.api_upload_flow(content, fname, sub_id, flow_key, lineas)
                
                # Icono según resultado
                icon = "✅" if "Exitoso" in res['status'] else "⚠️"
                logs_expander.write(f"{icon} **{fname}**: {res['status']} (Rec: {res['rec']})")
                
                audit_rows.append({
                    "Archivo": fname, "Sistema": sys_name, "Estado": res['status'],
                    "Detalles": res['details'], "Proc": res['proc'], "Rec": res['rec']
                })
        
        progress_bar.progress((i + 1) / total_files)

    logs_expander.update(label="✅ Proceso Finalizado", state="complete", expanded=False)
    status_text.success("¡Todos los archivos han sido procesados!")

    # --- RESULTADOS FINALES ---
    st.divider()
    
    if audit_rows:
        df = pd.DataFrame(audit_rows)
        
        # 1. Métricas visuales
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Archivos", len(df))
        m2.metric("Procesados OK", df[df['Estado'].str.contains("Exitoso")].shape[0])
        m3.metric("Registros (API)", df['Proc'].sum())
        m4.metric("IDs Reconciliados", df['Rec'].sum())
        
        # 2. Tabla con Colores
        def color_row(val):
            color = 'black'
            if 'Exitoso' in val: color = '#28a745' # Verde
            elif 'Fallos' in val: color = '#ffc107' # Amarillo
            elif 'Error' in val: color = '#dc3545' # Rojo
            elif 'Omitido' in val: color = '#6c757d' # Gris
            return f'color: {color}; font-weight: bold'

        st.subheader("📊 Reporte Detallado")
        st.dataframe(
            df.style.map(color_row, subset=['Estado']),
            use_container_width=True,
            column_config={
                "Proc": st.column_config.NumberColumn("Procesados"),
                "Rec": st.column_config.NumberColumn("Reconciliados"),
            }
        )
        
        # 3. Botón Descarga
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
            
        st.download_button(
            label="📥 Descargar Auditoría Excel",
            data=buffer.getvalue(),
            file_name="auditoria_final.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
    else:
        st.warning("No se generaron resultados.")
