# -*- coding: utf-8 -*-
"""
Aplicación Streamlit de procesamiento de pagos KashIO.
Versión optimizada para entornos corporativos.
"""

import streamlit as st
import requests
import pandas as pd
import ast
import re
from datetime import datetime

# ==========================================
# CONFIGURACIÓN DE PÁGINA
# ==========================================
st.set_page_config(page_title="Procesador KashIO", layout="centered")

# ==========================================
# CONSTANTES Y CREDENCIALES
# ==========================================
SERVICE_URL = "https://t5jezcpiwc.execute-api.us-east-1.amazonaws.com/LIVE"
HEADERS = {'content-type': 'application/json'}
AUTH_USER = ""  # Requerido: Ingresar credenciales
AUTH_PSW = ""   # Requerido: Ingresar credenciales

# ==========================================
# DATOS DE CUENTAS PSP
# ==========================================
DATA_PSP = [
    {'PSP': 'BCP','Currency': 'PEN','Descripcion': 'KashIO BCP PEN XXXX023','ACC': 'acc_R8eMfP7Cdaq5ScUavLXBMX'},
    {'PSP': 'BCP','Currency': 'USD','Descripcion': 'KashIO BCP USD XXXX178','ACC': 'acc_ddcdxWdBFhtwhWMJQKZ46h'},
    {'PSP': 'BBVA','Currency': 'PEN','Descripcion': 'KashIO BBVA PEN XXX0672','ACC': 'acc_cZpQmjzkKdoEvmWfj4Qm9K'},
    {'PSP': 'BBVA','Currency': 'USD','Descripcion': 'KashIO BBVA USD XXX0680','ACC': 'acc_S6tfrAUEnLcH2VNDiGt9d9'},
    {'PSP': 'IBK','Currency': 'PEN','Descripcion': 'KashIO IBK PEN XXXX1234','ACC': 'acc_rbzSXQuvc5FfMbR2z4vMGd'},
    {'PSP': 'IBK','Currency': 'USD','Descripcion': 'KashIO IBK USD XXXX6751','ACC': 'acc_9FmSeoNUiQTL7bHd2JzJCb'},
    {'PSP': 'SCOTIA','Currency': 'PEN','Descripcion': 'KashIO SCOTIA PEN XXXX0517','ACC': 'acc_R8eMfP7Cdaq5ScUavLXBMS'},
    {'PSP': 'SCOTIA','Currency': 'USD','Descripcion': 'KashIO SCOTIA USD XXXX','ACC': 'acc_cUtpw5swumqQRZDRZEcy23'},
    {'PSP': 'KASNET','Currency': 'PEN','Descripcion': 'KashIO KASNET PEN','ACC': 'acc_5w9ujngtRTf8qgG7xYyMJm'},
    {'PSP': 'BILLETERA-NIUBIZ','Currency': 'PEN','Descripcion': 'KashIO BILLETERAS PEN','ACC': 'acc_KCKUEA8gNiAk9r73zz6NMh'},
    {'PSP': 'CARD','Currency': 'PEN','Descripcion': 'Kashio Tarjetas','ACC': 'acc_8v3EMCq7EuuK2Czp54KF6e'},
    {'PSP': 'BILLETERA-GMONEY','Currency': 'PEN','Descripcion': 'KashIO BILLETERAS PEN','ACC': 'acc_24205fe371034deb9731'}
]
DICT_ACC = {(row['PSP'], row['Currency']): row['ACC'] for row in DATA_PSP}

# ==========================================
# FUNCIONES AUXILIARES (CORE BUSINESS LOGIC)
# ==========================================

def consultar_api_tins(tin_list):
    """Realiza consultas GET a la API para una lista de TINs y devuelve los resultados."""
    resultados = []
    
    with requests.Session() as session:
        session.auth = (AUTH_USER, AUTH_PSW)
        custom_headers = HEADERS.copy()
        custom_headers['User-Agent'] = 'PostmanRuntime/7.26.8'
        session.headers.update(custom_headers)
        
        progress = st.progress(0)
        total = len(tin_list)
        
        for idx, tin in enumerate(tin_list):
            url = f"{SERVICE_URL}/consultar/{tin}?search_by=PSP_TIN"
            try:
                response = session.get(url, timeout=15)
                if response.status_code in (200, 201):
                    resultados.append({"tin": tin, "data": response.json(), "error": None})
                else:
                    resultados.append({"tin": tin, "data": None, "error": f"Status {response.status_code}", "text": response.text})
            except Exception as e:
                resultados.append({"tin": tin, "data": None, "error": str(e)})
            
            progress.progress(int(((idx + 1) / total) * 100))
            
    progress.empty()
    return resultados

def procesar_archivo_bancario(file_content, filename):
    """Analiza un archivo TXT de recaudo y extrae información estructurada."""
    parsed_data = {}
    banco_detectado = "DESCONOCIDO"
    moneda_detectada = "PEN"
    
    lines = file_content.decode("utf-8", errors="ignore").splitlines()
    if not lines:
        return parsed_data, banco_detectado, moneda_detectada

    first_line = lines[0]
    
    # Identificación Inteligente de Banco y Moneda
    if first_line.startswith("0120"):
        banco_detectado = "BBVA"
        if "USD" in first_line[16:25]:
            moneda_detectada = "USD"
    elif first_line.startswith("0791501"):
        banco_detectado = "IBK"
        moneda_detectada = "PEN"
    elif first_line.startswith("0791502"):
        banco_detectado = "IBK"
        moneda_detectada = "USD"
    elif first_line.startswith("CC"):
        banco_detectado = "BCP"
        if "1941" in first_line[0:15] or "DOLARES" in filename.upper():
            moneda_detectada = "USD"

    # Extracción posicional específica
    for line in lines:
        try:
            tin, date_str, amount_str, op_str = None, None, None, None
            
            if banco_detectado == "IBK" and (line.startswith("0791501") or line.startswith("0791502")):
                if len(line) < 149: continue
                tin = line[37:49].strip()
                date_str = line[82:90]
                amount_str = line[96:109]
                op_str = line[141:149].strip()
                
            elif banco_detectado == "BBVA" and line.startswith("02"):
                if len(line) < 140: continue
                tin = line[48:60].strip()
                op_str = line[70:80].strip()
                amount_str = line[80:95]
                date_str = line[135:143] 
                
            elif banco_detectado == "BCP" and line.startswith("DD"):
                if len(line) < 150: continue
                tin = line[15:27].strip()
                date_str = line[57:65]
                amount_str = line[73:88]
                op_str = line[143:149].strip()
                
            if not tin or not re.match(r'^\d+$', tin):
                continue
                
            # Transformación de datos
            dt = datetime.strptime(date_str, "%Y%m%d")
            excel_date = str((dt - datetime(1899, 12, 30)).days)
            amount = float(amount_str) / 100.0
            
            parsed_data[tin] = {
                'VOUCHER_PSP': banco_detectado,
                'VOUCHER_Currency': moneda_detectada,
                'VOUCHER_Amount': amount,
                'VOUCHER_Operacion_PSP': op_str,
                'VOUCHER_FECHA': excel_date
            }
        except Exception:
            pass

    return parsed_data, banco_detectado, moneda_detectada

def ejecutar_post_pagos(df_validar, usuario_operacion):
    """Ejecuta los POST a la API para realizar los pagos manuales."""
    url_pago = f"{SERVICE_URL}/pagomanual"
    
    json_list = []
    status_list = []
    content_list = []
    
    with requests.Session() as session:
        session.auth = (AUTH_USER, AUTH_PSW)
        session.headers.update(HEADERS)
        
        progress = st.progress(0)
        total = len(df_validar)
        
        for index, row in df_validar.iterrows():
            psp = row['VOUCHER_PSP']
            psp_tin = row['VOUCHER_PSP_TIN']
            currency = row['VOUCHER_Currency']
            amount = row['VOUCHER_Amount']
            acc = DICT_ACC.get((psp, currency))
            
            if not acc:
                json_list.append(None)
                status_list.append(None)
                content_list.append(None)
                progress.progress((index + 1) / total)
                continue

            payload = {
                "invoice": {"id": psp_tin},
                "payer": {"reference_number": psp_tin},
                "amount": {"value": float(amount), "currency": currency},
                "psp_account": {"id": acc},
                "operation_no": row['VOUCHER_Operacion_PSP'],
                "operation_date": row['VOUCHER_FECHA'],
                "branch_code": "",
                "channel_code": "WEB",
                "force_expire_payment": True,
                "metadata": {"code": usuario_operacion, "clave": "AR"} 
            }

            # Extensión de datos si el PSP lo requiere
            if psp == 'BILLETERA-NIUBIZ':
                payload["metadata"].update({
                    "payment_method": row.get('QR_payment_method', ''),
                    "purchase_number": row.get('QR_purchase_number', ''),
                    "transaction_id": row.get('QR_transaction_id', ''),
                    "authorization_code": row.get('QR_authorization_code', ''),
                    "action_code": row.get('QR_action_code', ''),
                    "authorization_status": row.get('QR_authorization_status', ''),
                    "wallet": row.get('QR_WALLET', '')
                })
            elif psp == 'CARD':
                payload["metadata"].update({
                    "payment_method": "CARD",
                    "type": "xxxxxx",  
                    "brand": "xxxxxxx",
                    "provider": "*******",
                    "category": "xxxxx"
                })

            json_list.append(payload)

            try:
                response = session.post(url_pago, timeout=30, json=payload)
                status_list.append(response.status_code)
                content_list.append(response.json() if response.text.strip() else None)
            except Exception:
                status_list.append(None)
                content_list.append(None)
                
            progress.progress((index + 1) / total)
            
    progress.empty()
    df_validar['PAGAR_JSON'] = json_list
    df_validar['PAGAR_RESPONSE_STATUS'] = status_list
    df_validar['PAGAR_RESPONSE_CONTENT'] = content_list
    
    return df_validar

def cruzar_invoices_y_vouchers(data_voucher, api_results):
    """Compara los datos de vouchers proporcionados con los resultados de la API y valida discrepancias."""
    df_voucher = pd.DataFrame(data_voucher)
    
    invoices_data = []
    for res in api_results:
        data = res.get('data')
        if data:
            invoices_data.append({
                'INVOICE_ID': data.get("id"),
                'INVOICE_PSP_TIN': data.get("psp_tin"),
                'INVOICE_AMOUNT': data.get("total", {}).get("value"),
                'INVOICE_CURRENCY': data.get("total", {}).get("currency"),
                'INVOICE_STATUS': data.get("status")
            })
            
    df_invoices = pd.DataFrame(invoices_data, columns=['INVOICE_ID', 'INVOICE_PSP_TIN', 'INVOICE_AMOUNT', 'INVOICE_CURRENCY', 'INVOICE_STATUS'])
    
    if df_invoices.empty:
        return pd.DataFrame()
        
    df_validar = pd.merge(df_voucher, df_invoices, how='left', left_on=['VOUCHER_PSP_TIN'], right_on=['INVOICE_PSP_TIN'])
    df_validar['Validar_Currency'] = df_validar['VOUCHER_Currency'] == df_validar['INVOICE_CURRENCY']
    df_validar['Validar_Montos'] = df_validar['VOUCHER_Amount'] == df_validar['INVOICE_AMOUNT']

    df_coincidencias = df_validar[
        (df_validar['Validar_Currency'] == True) & 
        (df_validar['Validar_Montos'] == True) & 
        (df_validar['INVOICE_STATUS'] != "PAID")
    ].copy()
    
    return df_coincidencias

# ==========================================
# ESTADOS DE LA SESIÓN (SESSION STATE)
# ==========================================
if "tin_search_results" not in st.session_state:
    st.session_state.tin_search_results = None
if "trama_final_lista" not in st.session_state:
    st.session_state.trama_final_lista = None

# ==========================================
# INTERFAZ DE USUARIO (UI)
# ==========================================
st.title("Procesamiento de Pagos KashIO")
st.markdown("Plataforma operativa para consulta y regularización de operaciones.")

tab_consultar, tab_pagar = st.tabs(["Consulta y Procesamiento Automático", "Procesamiento Manual"])

# ==========================================
# PESTAÑA 1: CONSULTAR Y PROCESAR AUTOMÁTICAMENTE
# ==========================================
with tab_consultar:
    st.subheader("1. Consulta de estado de operaciones (TIN)")
    raw_input = st.text_area(
        "Ingrese los PSP_TIN (separados por comas o saltos de línea)",
        placeholder="260167375904\n260178160022",
        height=100
    )
    
    if st.button("Ejecutar Consulta", type="primary"):
        st.session_state.trama_final_lista = None
        tin_candidates = re.findall(r'\d+', raw_input)
        
        if not tin_candidates:
            st.warning("No se detectaron códigos TIN válidos en la entrada proporcionada.")
            st.session_state.tin_search_results = None
        else:
            seen = set()
            tin_list = [t for t in tin_candidates if not (t in seen or seen.add(t))]
            
            with st.spinner("Consultando información en la API..."):
                st.session_state.tin_search_results = consultar_api_tins(tin_list)
            st.success("Consulta completada de manera exitosa.")

    if st.session_state.tin_search_results:
        st.markdown("---")
        st.markdown("### Resumen de Consulta API")
        
        hay_tins_pendientes = False
        
        for res in st.session_state.tin_search_results:
            tin_clean = res["tin"]
            data = res["data"]
            
            if data is not None:
                status_factura = data.get("status", "")
                order_name = data.get("name") or data.get("metadata", {}).get("order_name", "Registro sin nombre")
                
                if status_factura == "PAID":
                    paid_date = "Fecha no registrada"
                    for act in (data.get("activity_list") or []):
                        if isinstance(act, dict) and act.get("status") == "PAID":
                            paid_date = act.get("created", paid_date)
                            break
                    st.success(f"TIN: {tin_clean} | {order_name} | ESTADO: PAGADO | Fecha registro: {paid_date}")
                else:
                    hay_tins_pendientes = True
                    activity_list = data.get("activity_list") or []
                    activity_desc = "Sin actividad registrada"
                    activity_status = "Desconocido"

                    if isinstance(activity_list, list) and activity_list:
                        first_act = next((act for act in activity_list if isinstance(act, dict) and (act.get("description") or act.get("status") or act.get("name"))), None)
                        if first_act:
                            activity_desc = first_act.get("description") or first_act.get("name") or "Sin descripción"
                            activity_status = first_act.get("status") or "Desconocido"

                    st.info(f"TIN: {tin_clean} | {order_name} | Actividad: {activity_desc} | Estado actual: {activity_status}")
            else:
                st.error(f"TIN: {tin_clean} | Error en consulta | {res.get('error')}")

        if hay_tins_pendientes:
            st.markdown("---")
            st.subheader("2. Carga de archivo de recaudo")
            st.write("Adjunte el archivo TXT bancario correspondiente para generar la trama de pago automático.")
            
            uploaded_file = st.file_uploader("Archivo de recaudo (.TXT)", type=['txt'])

            if st.button("Generar Trama de Datos", type="secondary"):
                trama_generada = []
                parsed_txt_data = {}
                
                if uploaded_file is not None:
                    parsed_txt_data, banco, moneda = procesar_archivo_bancario(uploaded_file.getvalue(), uploaded_file.name)
                    if banco != "DESCONOCIDO":
                        st.success(f"Archivo procesado: Origen {banco} - Moneda {moneda}")
                    else:
                        st.warning("El formato del archivo no coincide con los estándares conocidos (BCP, BBVA, IBK).")
                else:
                    st.warning("Procediendo sin archivo adjunto. Se requerirá completar la información manualmente.")

                # Consolidación de datos
                for res in st.session_state.tin_search_results:
                    tin_clean = res["tin"]
                    data = res["data"]
                    
                    if data is not None and data.get("status") != "PAID":
                        if tin_clean in parsed_txt_data:
                            txt_info = parsed_txt_data[tin_clean]
                            trama_generada.append({
                                'VOUCHER_PSP': txt_info['VOUCHER_PSP'],
                                'VOUCHER_PSP_TIN': tin_clean,
                                'VOUCHER_Currency': txt_info['VOUCHER_Currency'],
                                'VOUCHER_Amount': txt_info['VOUCHER_Amount'],
                                'VOUCHER_Operacion_PSP': txt_info['VOUCHER_Operacion_PSP'],
                                'VOUCHER_FECHA': txt_info['VOUCHER_FECHA']
                            })
                        else:
                            monto_api = data.get("total", {}).get("value", 0)
                            moneda_api = data.get("total", {}).get("currency", "")
                            trama_generada.append({
                                'VOUCHER_PSP': 'COMPLETAR_BANCO',
                                'VOUCHER_PSP_TIN': tin_clean,
                                'VOUCHER_Currency': moneda_api,
                                'VOUCHER_Amount': monto_api,
                                'VOUCHER_Operacion_PSP': 'COMPLETAR_OPERACION',
                                'VOUCHER_FECHA': 'COMPLETAR_FECHA'
                            })

                st.session_state.trama_final_lista = trama_generada

        else:
            st.info("No existen operaciones pendientes de regularización para los TINs consultados.")

        # PROCESAMIENTO FINAL EN PESTAÑA 1
        if st.session_state.trama_final_lista is not None and len(st.session_state.trama_final_lista) > 0:
            st.markdown("---")
            st.subheader("3. Ejecución de Pagos")
            
            df_preview = pd.DataFrame(st.session_state.trama_final_lista)
            st.dataframe(df_preview, use_container_width=True)
            
            col_u1, col_u2 = st.columns([1, 2])
            with col_u1:
                usuario_op_tab1 = st.text_input("Iniciales del Usuario Operativo", value="KNC", key="user_op_tab1")
            
            if st.button("Procesar Operaciones", type="primary", key="btn_procesar_tab1"):
                if not usuario_op_tab1.strip():
                    st.error("Es obligatorio ingresar las iniciales del usuario operativo.")
                    st.stop()
                    
                if any(t.get('VOUCHER_PSP') == 'COMPLETAR_BANCO' for t in st.session_state.trama_final_lista):
                    st.error("Se detectaron registros con información incompleta. Ajuste la trama o verifique el archivo de recaudo.")
                    st.stop()

                with st.spinner("Ejecutando cruce de información y procesamiento de pagos..."):
                    df_coincidencias = cruzar_invoices_y_vouchers(st.session_state.trama_final_lista, st.session_state.tin_search_results)
                    
                    if df_coincidencias.empty:
                        st.warning("No existen coincidencias válidas para procesar. Verifique posibles discrepancias en montos o divisas.")
                        st.stop()

                    df_resultados = ejecutar_post_pagos(df_coincidencias, usuario_op_tab1)

                st.subheader("Reporte de Resultados")
                pagos_exitosos = df_resultados[df_resultados['PAGAR_RESPONSE_STATUS'] == 200]
                pagos_fallidos = df_resultados[df_resultados['PAGAR_RESPONSE_STATUS'] != 200]

                col_res1, col_res2 = st.columns(2)
                col_res1.metric(label="Operaciones Exitosas", value=len(pagos_exitosos))
                col_res2.metric(label="Operaciones Fallidas", value=len(pagos_fallidos))

                if not pagos_exitosos.empty:
                    st.success("Las operaciones fueron procesadas correctamente en el sistema central.")
                if not pagos_fallidos.empty:
                    st.error("Se registraron errores durante el procesamiento de algunas operaciones. Revise el detalle.")
                    
                st.dataframe(
                    df_resultados[['VOUCHER_PSP_TIN', 'PAGAR_RESPONSE_STATUS', 'INVOICE_AMOUNT', 'INVOICE_CURRENCY']], 
                    use_container_width=True
                )

# ==========================================
# PESTAÑA 2: PROCESAMIENTO MANUAL
# ==========================================
with tab_pagar:
    st.subheader("Ingreso directo de estructura de datos (Trama)")
    st.write("Utilice este apartado para procesar transacciones omitiendo el flujo de validación automática documental.")
    
    col1, col2 = st.columns([1, 2])
    with col1:
        usuario_operacion = st.text_input("Iniciales del Usuario Operativo", value="KNC", key="user_op_tab2")
    
    trama_ejemplo = "[{'VOUCHER_PSP':'IBK','VOUCHER_PSP_TIN': '261122515932','VOUCHER_Currency': 'PEN','VOUCHER_Amount': 160,'VOUCHER_Operacion_PSP': '00005843','VOUCHER_FECHA':'46134'}]"
    trama_texto = st.text_area("Estructura de Vouchers (Formato Lista/Diccionario)", value=trama_ejemplo, height=200)

    if st.button("Procesar Operaciones", type="primary", key="btn_procesar_pagos_tab2"):
        if not usuario_operacion.strip():
            st.error("Es obligatorio ingresar las iniciales del usuario operativo.")
            st.stop()
            
        if not trama_texto.strip():
            st.error("El campo de estructura no puede encontrarse vacío.")
            st.stop()

        try:
            data_voucher_manual = ast.literal_eval(trama_texto)
            if isinstance(data_voucher_manual, dict):
                data_voucher_manual = [data_voucher_manual]
            if not isinstance(data_voucher_manual, list) or len(data_voucher_manual) == 0:
                raise ValueError("El formato provisto no cumple con la estructura requerida.")
        except Exception as e:
            st.error(f"Error de validación de estructura: {e}")
            st.stop()

        df_voucher_prev = pd.DataFrame(data_voucher_manual)
        with st.expander("Detalle de registros a procesar", expanded=False):
            st.dataframe(df_voucher_prev, use_container_width=True)

        with st.spinner("Validando registros con el sistema central..."):
            tin_list_manual = df_voucher_prev['VOUCHER_PSP_TIN'].dropna().unique().tolist()
            api_results_manual = consultar_api_tins(tin_list_manual)
            
            df_coincidencias = cruzar_invoices_y_vouchers(data_voucher_manual, api_results_manual)

            if df_coincidencias.empty:
                st.warning("Validación denegada: Discrepancias detectadas o los comprobantes se encuentran en estado liquidado.")
                st.stop()

        with st.spinner("Procesando transacciones..."):
            df_resultados = ejecutar_post_pagos(df_coincidencias, usuario_operacion)

        st.subheader("Reporte de Resultados")
        pagos_exitosos = df_resultados[df_resultados['PAGAR_RESPONSE_STATUS'] == 200]
        pagos_fallidos = df_resultados[df_resultados['PAGAR_RESPONSE_STATUS'] != 200]

        col_res1, col_res2 = st.columns(2)
        col_res1.metric(label="Operaciones Exitosas", value=len(pagos_exitosos))
        col_res2.metric(label="Operaciones Fallidas", value=len(pagos_fallidos))

        if not pagos_exitosos.empty:
            st.success("Las operaciones fueron procesadas correctamente.")
        if not pagos_fallidos.empty:
            st.error("Se registraron anomalías en el procesamiento. Verifique los códigos de estado.")
            
        st.dataframe(
            df_resultados[['VOUCHER_PSP_TIN', 'PAGAR_RESPONSE_STATUS', 'INVOICE_AMOUNT', 'INVOICE_CURRENCY']], 
            use_container_width=True
        )
