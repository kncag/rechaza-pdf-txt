import streamlit as st
import pandas as pd
from datetime import date
from io import BytesIO
import logic_processor as logic
from imap_tools import MailBox

# Configuración de la página
st.set_page_config(page_title="Robot de Conciliación", page_icon="🤖", layout="wide")

# --- CSS para estilo ---
st.markdown("""
<style>
    .stButton>button { width: 100%; background-color: #0068c9; color: white; }
    .success { color: green; font-weight: bold; }
    .error { color: red; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# --- Sidebar: Configuración ---
with st.sidebar:
    st.header("⚙️ Configuración")
    user_email = st.text_input("Correo Electrónico", "conciliacion@kashio.net")
    # Usamos type="password" para ocultarla
    user_pass = st.text_input("Contraseña (App Password)", type="password")
    
    st.markdown("---")
    st.subheader("Parámetros")
    target_date = st.date_input("Fecha a Procesar", date.today())
    
    st.markdown("---")
    st.info("Nota: Asegúrate de usar una Contraseña de Aplicación si tienes MFA activado.")

# --- Panel Principal ---
st.title("🤖 Automatización de Conciliación")
st.markdown(f"**Fecha seleccionada:** {target_date.strftime('%d/%m/%Y')} (Ventana 19:00 anterior - 19:00 actual)")

# Contenedor para resultados
if "audit_data" not in st.session_state:
    st.session_state.audit_data = []

# --- BOTÓN DE EJECUCIÓN ---
if st.button("🚀 INICIAR PROCESO", type="primary"):
    if not user_pass:
        st.error("⚠️ Por favor ingresa la contraseña del correo.")
        st.stop()

    st.session_state.audit_data = [] # Limpiar auditoría anterior
    audit_rows = []
    
    # 1. Conexión y Descarga
    try:
        with st.status("Conectando a Outlook y descargando correos...", expanded=True) as status:
            mailbox = logic.conectar_imap(user_email, user_pass)
            status.write("✅ Conexión IMAP exitosa.")
            
            # Aquí iteramos sobre las carpetas que te interesan
            # Nota: En IMAP, necesitas saber el nombre exacto de la carpeta en el servidor
            # A veces es "Inbox/01) EURO" o simplemente "01) EURO".
            # Para este ejemplo, buscamos en INBOX y filtramos, o puedes adaptar logic.descargar_adjuntos
            # para seleccionar carpeta: mailbox.folder.set('NombreCarpeta')
            
            # SIMULACIÓN DE FLUJO DE CARPETAS
    # --- CONFIGURACIÓN ACTUALIZADA DE CARPETAS ---
            # Nota: Si falla con "Bandeja de entrada", intenta cambiarlo por "INBOX"
            # Ejemplo: "INBOX/REC/EURO"
            
            folders_to_check = {
                "EURO": {
                    "imap_folder": "Bandeja de entrada/REC/EURO", 
                    "flow": "euro", 
                    "rules": logic.RULES_EURO
                },
                "UDEP": {
                    "imap_folder": "Bandeja de entrada/REC/UDEP", 
                    "flow": "udep", 
                    "rules": logic.RULES_UDEP
                }
            }
            
            processed_count = 0
            
            for system_name, cfg in folders_to_check.items():
                status.write(f"📂 Entrando a carpeta: **{cfg['imap_folder']}**...")
                
                try:
                    # Intentamos seleccionar la carpeta específica
                    mailbox.folder.set(cfg['imap_folder'])
                except Exception as e:
                    # Si falla, mostramos el error y probamos con la ruta en inglés por si acaso
                    status.warning(f"No se encontró '{cfg['imap_folder']}'. Intentando ruta alternativa...")
                    try:
                        # Intento alternativo (común en servidores Exchange)
                        alt_folder = cfg['imap_folder'].replace("Bandeja de entrada", "INBOX")
                        mailbox.folder.set(alt_folder)
                        status.info(f"Conectado a '{alt_folder}'")
                    except:
                        status.error(f"❌ No se pudo encontrar la carpeta: {cfg['imap_folder']}")
                        continue # Saltamos al siguiente sistema

                # Descargar
                atts = logic.descargar_adjuntos(mailbox, target_date, [])
                
                for att in atts:
                    fname = att['filename']
                    fcontent = att['content']
                    
                    # Clasificación
                    sub_id = logic.match_subscription(fname, cfg['rules'])
                    
                    if sub_id:
                        status.write(f"⚡ Procesando: `{fname}` -> API ({sub_id})")
                        
                        # Subida a API
                        api_res = logic.api_upload_flow(fcontent, fname, sub_id, cfg['flow'])
                        
                        # Guardar resultado
                        row = {
                            "Sistema": system_name,
                            "Archivo": fname,
                            "Subscripción": sub_id,
                            "Estado": api_res['status'],
                            "Procesados": api_res.get('processed', 0),
                            "Fallidos": api_res.get('failed', 0),
                            "Reconciliados": api_res.get('reconciled', 0),
                            "Detalles": api_res['details'],
                            "Timestamp": datetime.now().strftime("%H:%M:%S")
                        }
                        audit_rows.append(row)
                        processed_count += 1
                    else:
                        # No match
                        pass
            
            status.update(label="¡Proceso Completado!", state="complete", expanded=False)
            st.session_state.audit_data = audit_rows

            if processed_count == 0:
                st.warning("No se encontraron archivos válidos o coincidencias de reglas en la fecha seleccionada.")
            else:
                st.success(f"Se procesaron {processed_count} archivos correctamente.")

    except Exception as e:
        st.error(f"Ocurrió un error crítico: {e}")

# --- Mostrar Resultados ---
if st.session_state.audit_data:
    st.subheader("📊 Resultados de Auditoría")
    df = pd.DataFrame(st.session_state.audit_data)
    
    # Métricas rápidas
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Archivos", len(df))
    c2.metric("Registros Procesados", df['Procesados'].sum())
    c3.metric("IDs Reconciliados", df['Reconciliados'].sum())
    
    # Tabla interactiva
    st.dataframe(df, use_container_width=True)
    
    # Botón de descarga Excel
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Auditoria')
    
    st.download_button(
        label="📥 Descargar Reporte Excel",
        data=buffer.getvalue(),
        file_name=f"auditoria_{target_date}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
