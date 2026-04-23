# -*- coding: utf-8 -*-
"""
Aplicación Streamlit de procesamiento de pagos KashIO
Optimizado para evitar redundancias, usar connection pooling (Session) y búsquedas eficientes.
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
st.set_page_config(page_title="KashIO Processor", page_icon="💸", layout="wide")

# ==========================================
# CONSTANTES Y CREDENCIALES
# ==========================================
_service_url = "https://t5jezcpiwc.execute-api.us-east-1.amazonaws.com/LIVE"
_headers = {'content-type': 'application/json'}
_auth_user = ""  # <- RELLENAR AQUÍ
_auth_psw = ""   # <- RELLENAR AQUÍ

# ==========================================
# DATOS DE CUENTAS PSP
# ==========================================
data_PSP = [
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
dict_acc = {(row['PSP'], row['Currency']): row['ACC'] for row in data_PSP}

# ==========================================
# ESTADOS DE LA SESIÓN (SESSION STATE)
# ==========================================
if "tin_search_results" not in st.session_state:
    st.session_state.tin_search_results = None

# ==========================================
# INTERFAZ DE USUARIO (UI)
# ==========================================
st.title("💸 Herramientas KashIO")
st.markdown("Usa las pestañas inferiores para navegar entre la consulta de estados y el procesamiento de pagos manuales.")

# Crear pestañas para separar las funcionalidades
tab_consultar, tab_pagar = st.tabs(["🔎 Consultar Estado de TINs", "🚀 Procesar Pagos Manuales"])

# ==========================================
# PESTAÑA 1: CONSULTAR TINS
# ==========================================
with tab_consultar:
    st.subheader("Consulta múltiple de estados")
    raw_input = st.text_area(
        "Pega aquí uno o varios PSP_TIN (separados por comas, espacios o saltos de línea)",
        placeholder="260167375904, 260178160022\n260178163684",
        height=150
    )
    
    if st.button("Consultar Ahora", key="btn_final_v10_multi", type="primary"):
        # Extraer solo los números de la entrada
        tin_candidates = re.findall(r'\d+', raw_input)
        
        if not tin_candidates:
            st.warning("No se detectaron TINs válidos en la entrada. Asegúrate de incluir números.")
            st.session_state.tin_search_results = None
        else:
            # Eliminar duplicados manteniendo el orden
            seen = set()
            tin_list = [t for t in tin_candidates if not (t in seen or seen.add(t))]
            
            formatted = ",".join([f'"{tin}"' for tin in tin_list])
            st.caption("🔎 TINs detectados y formateados:")
            st.code(formatted)
            
            progress = st.progress(0)
            total = len(tin_list)
            resultados_memoria = []
            
            # Usar Session para Connection Pooling en las consultas
            with requests.Session() as session:
                session.auth = (_auth_user, _auth_psw)
                headers_postman = _headers.copy()
                headers_postman['User-Agent'] = 'PostmanRuntime/7.26.8'
                session.headers.update(headers_postman)
                
                for idx, tin_clean in enumerate(tin_list):
                    url_postman = f"{_service_url}/consultar/{tin_clean}?search_by=PSP_TIN"
                    
                    try:
                        r = session.get(url_postman, timeout=15)
                        if r.status_code == 200:
                            resultados_memoria.append({"tin": tin_clean, "data": r.json(), "error": None})
                        else:
                            resultados_memoria.append({"tin": tin_clean, "data": None, "error": f"Status {r.status_code}", "text": r.text})
                    except Exception as e:
                        resultados_memoria.append({"tin": tin_clean, "data": None, "error": str(e)})
                    
                    # Actualizar barra de progreso
                    progress.progress(int(((idx + 1) / total) * 100))
            
            st.session_state.tin_search_results = resultados_memoria
            st.success("Consulta finalizada.")

    # Mostrar resultados guardados en memoria (sobreviven a recargas de otros widgets)
    if st.session_state.tin_search_results:
        st.markdown("---")
        for res in st.session_state.tin_search_results:
            tin_clean = res["tin"]
            data = res["data"]
            
            if data is not None:
                # Extraemos el nombre y monto
                order_name = data.get("name") or data.get("metadata", {}).get("order_name", "SIN NOMBRE")
                monto_encontrado = data.get("total", {}).get("value", 0)
                moneda_encontrada = data.get("total", {}).get("currency", "")

                activity_list = data.get("activity_list") or []
                activity_desc = "SIN ACTIVITY"
                activity_status = "SIN ESTADO"

                if isinstance(activity_list, list) and activity_list:
                    first_act = None
                    for act in activity_list:
                        if isinstance(act, dict) and (act.get("description") or act.get("status") or act.get("name")):
                            first_act = act
                            break
                    if first_act:
                        activity_desc = first_act.get("description") or first_act.get("name") or "SIN DESCRIPCIÓN"
                        activity_status = first_act.get("status") or "SIN ESTADO"

                # Formato visual
                header_text = f"✅ {tin_clean} | {order_name} | {activity_desc} | estado: {activity_status} | Monto: {monto_encontrado}"
                st.subheader(header_text)
                
                # --- NUEVA FUNCIONALIDAD: PLANTILLA SUGERIDA ---
                st.markdown("**Plantilla sugerida para la pestaña de Pagos:** *(Copia y completa los datos faltantes)*")
                plantilla_sugerida = {
                    'VOUCHER_PSP': 'COMPLETAR_BANCO (Ej: IBK)',
                    'VOUCHER_PSP_TIN': tin_clean,
                    'VOUCHER_Currency': moneda_encontrada,
                    'VOUCHER_Amount': monto_encontrado,
                    'VOUCHER_Operacion_PSP': 'COMPLETAR_OPERACION',
                    'VOUCHER_FECHA': 'COMPLETAR_FECHA'
                }
                st.code(str(plantilla_sugerida), language='python')
                
                with st.expander("Ver JSON Completo"):
                    st.json(data)
            else:
                st.error(f"❌ {tin_clean} | Error: {res.get('error')} | {res.get('text', '')}")

# ==========================================
# PESTAÑA 2: PROCESAR PAGOS
# ==========================================
with tab_pagar:
    st.subheader("Procesamiento Masivo de Vouchers")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        usuario_operacion = st.text_input("Iniciales de Usuario (_USUARIO_OPERACION)", value="KNC")
    
    with col2:
        input_method = st.radio("Método de entrada de Vouchers:", ["Subir Archivo TXT (Recaudo Banco)", "Pegar Trama Manualmente"], horizontal=True)

    data_VOUCHER = []

    # Lógica 1: Leer desde un archivo TXT adjunto
    if input_method == "Subir Archivo TXT (Recaudo Banco)":
        col_psp, col_cur, col_file = st.columns([1, 1, 2])
        with col_psp:
            file_psp = st.selectbox("Banco Origen (PSP)", ["BCP", "BBVA", "IBK", "SCOTIA", "KASNET"])
        with col_cur:
            file_currency = st.selectbox("Moneda del Archivo", ["PEN", "USD"])
        with col_file:
            uploaded_file = st.file_uploader("Sube el archivo .TXT", type=['txt'])
        
        if uploaded_file is not None:
            lines = uploaded_file.getvalue().decode("utf-8", errors="ignore").splitlines()
            for line in lines:
                # Validar que la línea tenga la longitud suficiente para ser un registro de pago
                if len(line.strip()) >= 149:
                    try:
                        # Parseando el formato posicional del TXT
                        tin = line[37:49].strip()
                        date_str = line[82:90]
                        amount_str = line[96:109]
                        op_str = line[141:149]
                        
                        # Convertir fecha de YYYYMMDD a formato Serial Excel (Ej. 46134)
                        dt = datetime.strptime(date_str, "%Y%m%d")
                        excel_date = str((dt - datetime(1899, 12, 30)).days)
                        
                        # Convertir monto (13 caracteres: 11 enteros, 2 decimales)
                        amount = float(amount_str) / 100.0
                        
                        data_VOUCHER.append({
                            'VOUCHER_PSP': file_psp,
                            'VOUCHER_PSP_TIN': tin,
                            'VOUCHER_Currency': file_currency,
                            'VOUCHER_Amount': amount,
                            'VOUCHER_Operacion_PSP': op_str,
                            'VOUCHER_FECHA': excel_date
                        })
                    except Exception as e:
                        pass # Ignorar líneas de cabecera o pie de página que no cumplan el formato
                        
            if not data_VOUCHER:
                st.warning("No se pudo extraer información. Verifica que sea el formato TXT de recaudo bancario correcto.")
            else:
                st.success(f"✅ ¡Éxito! Se extrajeron {len(data_VOUCHER)} vouchers del archivo correctamente.")
                
    # Lógica 2: Leer desde trama de texto pegada manualmente
    else:
        trama_ejemplo = "[{'VOUCHER_PSP':'IBK','VOUCHER_PSP_TIN': '261122515932','VOUCHER_Currency': 'PEN','VOUCHER_Amount': 160,'VOUCHER_Operacion_PSP': '00005843','VOUCHER_FECHA':'46134'}]"
        trama_texto = st.text_area("Pegar Trama de Vouchers (Diccionario o Lista Python)", value=trama_ejemplo, height=150)
        
        if trama_texto.strip():
            try:
                data_VOUCHER = ast.literal_eval(trama_texto)
                if isinstance(data_VOUCHER, dict):
                    data_VOUCHER = [data_VOUCHER]
                if not isinstance(data_VOUCHER, list) or len(data_VOUCHER) == 0:
                    st.error("El formato debe ser un diccionario o una lista de diccionarios.")
                    data_VOUCHER = []
            except Exception as e:
                st.error(f"❌ Error al leer la trama. Verifica el formato. Detalle: {e}")
                data_VOUCHER = []

    # Botón para ejecutar el proceso masivo
    if st.button("🚀 Procesar Pagos", type="primary", key="btn_procesar_pagos"):
        
        if not usuario_operacion.strip():
            st.error("Por favor, ingresa las iniciales del usuario.")
            st.stop()
            
        if not data_VOUCHER:
            st.error("No hay vouchers para procesar. Sube un archivo TXT válido o pega una trama.")
            st.stop()

        df_VOUCHER = pd.DataFrame(data_VOUCHER)
        with st.expander("Vouchers Leídos (Vista Previa)", expanded=False):
            st.dataframe(df_VOUCHER, use_container_width=True)

        with st.spinner('Consultando Invoices en API...'):
            # 2. CONSULTA DE INVOICES (GET)
            _ListaColumnas = ['INVOICE_ID', 'INVOICE_PSP_TIN', 'INVOICE_AMOUNT', 'INVOICE_CURRENCY', 'INVOICE_STATUS']
            invoices_data = []

            with requests.Session() as session:
                session.auth = (_auth_user, _auth_psw)
                session.headers.update(_headers)
                
                for _, row in df_VOUCHER.iterrows():
                    voucher_PSP_TIN = row.get('VOUCHER_PSP_TIN')
                    if not voucher_PSP_TIN:
                        continue
                        
                    _url_consultar = f"{_service_url}/consultar/{voucher_PSP_TIN}?search_by=PSP_TIN"

                    try:
                        response = session.get(_url_consultar, timeout=5)
                        if response.status_code in (200, 201):
                            rsp = response.json()
                            invoices_data.append({
                                'INVOICE_ID': rsp.get("id"),
                                'INVOICE_PSP_TIN': rsp.get("psp_tin"),
                                'INVOICE_AMOUNT': rsp.get("total", {}).get("value"),
                                'INVOICE_CURRENCY': rsp.get("total", {}).get("currency"),
                                'INVOICE_STATUS': rsp.get("status")
                            })
                    except Exception as e:
                        st.warning(f"Error al consultar el TIN {voucher_PSP_TIN}: {e}")

            INVOICES = pd.DataFrame(invoices_data, columns=_ListaColumnas)

            # 3. VALIDACIÓN DE COINCIDENCIAS
            if INVOICES.empty:
                st.error("No se encontraron facturas (Invoices) en la API con los TINs proporcionados.")
                st.stop()

            VALIDAR = pd.merge(df_VOUCHER, INVOICES, how='outer', left_on=['VOUCHER_PSP_TIN'], right_on=['INVOICE_PSP_TIN'])
            VALIDAR['Validar_Currency'] = VALIDAR['VOUCHER_Currency'] == VALIDAR['INVOICE_CURRENCY']
            VALIDAR['Validar_Montos'] = VALIDAR['VOUCHER_Amount'] == VALIDAR['INVOICE_AMOUNT']

            VALIDAR_COINCIDE = VALIDAR[
                (VALIDAR['Validar_Currency'] == True) & 
                (VALIDAR['Validar_Montos'] == True) & 
                (VALIDAR['INVOICE_STATUS'] != "PAID")
            ].copy()

        st.info(f"Se encontraron **{len(VALIDAR_COINCIDE)}** coincidencias listas para pagar.")

        if VALIDAR_COINCIDE.empty:
            st.warning("No hay coincidencias válidas para procesar pagos el día de hoy (Revisa importes, monedas o si ya están 'PAID').")
            with st.expander("Ver tabla completa de cruce"):
                st.dataframe(VALIDAR, use_container_width=True)
            st.stop()

        # 4. PROCESAMIENTO DE PAGOS (POST)
        with st.spinner('Procesando Pagos Manuales...'):
            pagar_json_list = []
            pagar_status_list = []
            pagar_content_list = []

            _url_pago = f"{_service_url}/pagomanual"

            with requests.Session() as session:
                session.auth = (_auth_user, _auth_psw)
                session.headers.update(_headers)

                progress_bar = st.progress(0)
                total_filas = len(VALIDAR_COINCIDE)

                for index, (i, row) in enumerate(VALIDAR_COINCIDE.iterrows()):
                    _PSP = row['VOUCHER_PSP']
                    _PSP_TIN = row['VOUCHER_PSP_TIN']
                    _CURRENCY = row['VOUCHER_Currency']
                    _AMOUNT = row['VOUCHER_Amount']

                    _ACC = dict_acc.get((_PSP, _CURRENCY))
                    
                    if not _ACC:
                        st.warning(f"No se encontró cuenta de destino para {_PSP} y {_CURRENCY}")
                        pagar_json_list.append(None); pagar_status_list.append(None); pagar_content_list.append(None)
                        progress_bar.progress((index + 1) / total_filas)
                        continue

                    _json = {
                        "invoice": {"id": _PSP_TIN},
                        "payer": {"reference_number": _PSP_TIN},
                        "amount": {"value": float(_AMOUNT), "currency": _CURRENCY},
                        "psp_account": {"id": _ACC},
                        "operation_no": row['VOUCHER_Operacion_PSP'],
                        "operation_date": row['VOUCHER_FECHA'],
                        "branch_code": "",
                        "channel_code": "WEB",
                        "force_expire_payment": True,
                        "metadata": {"code": usuario_operacion, "clave": "AR"} 
                    }

                    if _PSP == 'BILLETERA-NIUBIZ':
                        _json["metadata"].update({
                            "payment_method": row.get('QR_payment_method', ''),
                            "purchase_number": row.get('QR_purchase_number', ''),
                            "transaction_id": row.get('QR_transaction_id', ''),
                            "authorization_code": row.get('QR_authorization_code', ''),
                            "action_code": row.get('QR_action_code', ''),
                            "authorization_status": row.get('QR_authorization_status', ''),
                            "wallet": row.get('QR_WALLET', '')
                        })
                    elif _PSP == 'CARD':
                        _json["metadata"].update({
                            "payment_method": "CARD",
                            "type": "xxxxxx",  
                            "brand": "xxxxxxx",
                            "provider": "*******",
                            "category": "xxxxx"
                        })

                    pagar_json_list.append(_json)

                    try:
                        response = session.post(_url_pago, timeout=30, json=_json)
                        pagar_status_list.append(response.status_code)
                        if response.text.strip():
                            pagar_content_list.append(response.json())
                        else:
                            pagar_content_list.append(None)
                    except Exception as e:
                        st.error(f"Error procesando pago para {_PSP_TIN}: {e}")
                        pagar_status_list.append(None)
                        pagar_content_list.append(None)
                    
                    progress_bar.progress((index + 1) / total_filas)

            VALIDAR_COINCIDE['PAGAR_JSON'] = pagar_json_list
            VALIDAR_COINCIDE['PAGAR_RESPONSE_STATUS'] = pagar_status_list
            VALIDAR_COINCIDE['PAGAR_RESPONSE_CONTENT'] = pagar_content_list

        # ==========================================
        # 5. MOSTRAR RESULTADOS
        # ==========================================
        st.subheader("📊 Resultados del Procesamiento")
        
        successful_payments = VALIDAR_COINCIDE[VALIDAR_COINCIDE['PAGAR_RESPONSE_STATUS'] == 200]
        failed_payments = VALIDAR_COINCIDE[VALIDAR_COINCIDE['PAGAR_RESPONSE_STATUS'] != 200]

        col_res1, col_res2 = st.columns(2)
        col_res1.metric(label="Pagos Exitosos", value=len(successful_payments))
        col_res2.metric(label="Pagos Fallidos", value=len(failed_payments))

        if not successful_payments.empty:
            st.success("¡Se procesaron pagos con éxito!")
            with st.expander("Ver Respuesta del Primer Pago Exitoso"):
                st.json(successful_payments['PAGAR_RESPONSE_CONTENT'].iloc[0])

        if not failed_payments.empty:
            st.error("Algunos pagos no devolvieron status 200.")
            
        st.write("**Resumen General de Operaciones:**")
        st.dataframe(
            VALIDAR_COINCIDE[['VOUCHER_PSP_TIN', 'PAGAR_RESPONSE_STATUS', 'INVOICE_AMOUNT', 'INVOICE_CURRENCY']], 
            use_container_width=True
        )
