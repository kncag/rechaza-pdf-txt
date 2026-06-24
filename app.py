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
from io import BytesIO  # <-- NUEVA IMPORTACIÓN PARA EXCEL EN MEMORIA

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
    
    raw_text = file_content.decode("latin-1", errors="replace")
    clean_text = re.sub(r'[^\x20-\x7E\r\n]', ' ', raw_text)
    
    lines = clean_text.splitlines()
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
                tin = re.sub(r'[^\d]', '', line[37:49])
                date_str = re.sub(r'[^\d]', '', line[82:90])
                amount_str = re.sub(r'[^\d]', '', line[96:109])
                op_str = line[141:149].strip()
                
            elif banco_detectado == "BBVA" and line.startswith("02"):
                if len(line) < 140: continue
                tin = re.sub(r'[^\d]', '', line[48:60])
                op_str = line[70:80].strip()
                amount_str = re.sub(r'[^\d]', '', line[80:95])
                date_str = re.sub(r'[^\d]', '', line[135:143]) 
                
            elif banco_detectado == "BCP" and line.startswith("DD"):
                if len(line) < 150: continue
                tin = re.sub(r'[^\d]', '', line[15:27])
                date_str = re.sub(r'[^\d]', '', line[57:65])
                amount_str = re.sub(r'[^\d]', '', line[73:88])
                op_str = line[143:149].strip()
                
            if not tin or not date_str or not amount_str:
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
if "procesando" not in st.session_state:
    st.session_state.procesando = False

# ==========================================
# INTERFAZ DE USUARIO (UI)
# ==========================================
st.title("Procesamiento de Pagos KashIO")
st.markdown("Plataforma operativa para consulta y regularización de operaciones.")

tab_consultar, tab_pagar, tab_consultar_tin_main = st.tabs(["Consulta y Procesamiento Automático", "Procesamiento Manual", "CONSULTAR TIN (Main)"])

# ==========================================
# PESTAÑA 1: CONSULTAR Y PROCESAR AUTOMÁTICAMENTE
# ==========================================
with tab_consultar:
    st.subheader("1. Consulta de estado de operaciones (TIN)")
    raw_input = st.text_area("Ingrese los PSP_TIN", height=100, key="txt_area_tab1")
    
    if st.button("Ejecutar Consulta", type="primary"):
        st.session_state.trama_final_lista = None
        tin_candidates = re.findall(r'\d+', raw_input)
        
        if not tin_candidates:
            st.warning("No se detectaron códigos.")
            st.session_state.tin_search_results = None
        else:
            tin_list = list(dict.fromkeys(tin_candidates))
            with st.spinner("Consultando..."):
                st.session_state.tin_search_results = consultar_api_tins(tin_list)
            st.success("Consulta completada.")

    if st.session_state.tin_search_results:
        hay_tins_pendientes = False
        for res in st.session_state.tin_search_results:
            if res["data"] is not None:
                if res["data"].get("status") != "PAID":
                    hay_tins_pendientes = True
                    break

        if hay_tins_pendientes:
            st.subheader("2. Carga de archivo de recaudo")
            uploaded_file = st.file_uploader("Archivo de recaudo (.TXT)", type=['txt'])

            if st.button("Generar Trama de Datos", type="secondary"):
                trama_generada = []
                parsed_txt_data = {}
                
                if uploaded_file is not None:
                    parsed_txt_data, b, m = procesar_archivo_bancario(uploaded_file.getvalue(), uploaded_file.name)

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
                            trama_generada.append({
                                'VOUCHER_PSP': 'COMPLETAR_BANCO',
                                'VOUCHER_PSP_TIN': tin_clean,
                                'VOUCHER_Currency': data.get("total", {}).get("currency", ""),
                                'VOUCHER_Amount': data.get("total", {}).get("value", 0),
                                'VOUCHER_Operacion_PSP': 'COMPLETAR_OPERACION',
                                'VOUCHER_FECHA': 'COMPLETAR_FECHA'
                            })

                st.session_state.trama_final_lista = trama_generada

        if st.session_state.trama_final_lista is not None and len(st.session_state.trama_final_lista) > 0:
            st.markdown("---")
            st.subheader("3. Ejecución de Pagos")
            st.dataframe(pd.DataFrame(st.session_state.trama_final_lista))
            
            usuario_op_tab1 = st.text_input("Iniciales del Usuario", value="KNC", key="u1")
            
            if st.button("Procesar Operaciones", type="primary", key="b1"):
                with st.spinner("Procesando..."):
                    df_coincidencias = cruzar_invoices_y_vouchers(st.session_state.trama_final_lista, st.session_state.tin_search_results)
                    df_resultados = ejecutar_post_pagos(df_coincidencias, usuario_op_tab1)
                st.dataframe(df_resultados[['VOUCHER_PSP_TIN', 'PAGAR_RESPONSE_STATUS']])

# ==========================================
# PESTAÑA 2: PROCESAMIENTO MANUAL
# ==========================================
with tab_pagar:
    st.subheader("Ingreso directo de estructura de datos (Trama)")
    usuario_operacion = st.text_input("Iniciales", value="KNC", key="u2")
    trama_texto = st.text_area("Trama", height=200)

    if st.button("Procesar", type="primary", key="b2"):
        data_voucher_manual = ast.literal_eval(trama_texto)
        if isinstance(data_voucher_manual, dict): data_voucher_manual = [data_voucher_manual]
        
        df_voucher_prev = pd.DataFrame(data_voucher_manual)
        
        with st.spinner("Procesando..."):
            api_results_manual = consultar_api_tins(df_voucher_prev['VOUCHER_PSP_TIN'].tolist())
            df_coincidencias = cruzar_invoices_y_vouchers(data_voucher_manual, api_results_manual)
            df_resultados = ejecutar_post_pagos(df_coincidencias, usuario_operacion)

        st.dataframe(df_resultados[['VOUCHER_PSP_TIN', 'PAGAR_RESPONSE_STATUS']])

# ==================================================
# PESTAÑA 3: CONSULTAR TIN (Mapeo dinámico a Tabla)
# ==================================================
with tab_consultar_tin_main:
    st.subheader("Buscador de Códigos PSP_TIN y Generador de Datos")

    uploaded_file_tab3 = st.file_uploader(
        "Adjunte el archivo TXT bancario para completar automáticamente Banco, Nro OP y VOUCHER_FECHA (Opcional)", 
        type=['txt'], 
        key="uploader_tab3"
    )
    
    parsed_txt_data = {}
    if uploaded_file_tab3 is not None:
        parsed_txt_data, _, _ = procesar_archivo_bancario(uploaded_file_tab3.getvalue(), uploaded_file_tab3.name)

    raw_input_tin = st.text_area(
        "Ingrese códigos (separados por comas o saltos de línea)", 
        placeholder="260167375904\n260178163684", 
        disabled=st.session_state.procesando, 
        key="txt_area_tab3"
    )
    
    if st.button("Consultar en Red", key="btn_consultar_tin", disabled=st.session_state.procesando):
        st.session_state.procesando = True
        raw_candidates = re.findall(r'\d+', raw_input_tin)
        tin_candidates = [t[2:] if (t.startswith("00") and len(t)>12) else t for t in raw_candidates if len(t)==12 or (t.startswith("00") and len(t[2:])==12)]
                
        if not tin_candidates:
            st.warning("No se detectaron códigos TIN válidos (12 dígitos).")
            st.session_state.tin_search_results = None
        else:
            tin_list = list(dict.fromkeys(tin_candidates))
            prog = st.progress(0); total = len(tin_list); resultados_memoria = []

            for idx, tin_clean in enumerate(tin_list, 1):
                try:
                    r = requests.get(f"{SERVICE_URL}/consultar/{tin_clean}?search_by=PSP_TIN", headers={'Content-Type':'text/plain', 'User-Agent':'PostmanRuntime'}, timeout=15)
                    resultados_memoria.append({"tin": tin_clean, "data": r.json() if r.status_code==200 else None, "error": r.status_code if r.status_code!=200 else None, "text": r.text})
                except Exception as e:
                    resultados_memoria.append({"tin": tin_clean, "data": None, "error": str(e)})
                prog.progress(int((idx/total)*100))
    
            st.session_state.tin_search_results = resultados_memoria
            st.success("Búsqueda y extracción finalizada.")
        st.session_state.procesando = False

    if st.session_state.tin_search_results:
        tabla_rows = []
        
        now = datetime.now()
        fecha_revision = now.strftime("%d/%m/%Y")
        mes_revision = now.strftime("%B")
        
        for res in st.session_state.tin_search_results:
            t = res["tin"]
            d = res["data"]
            
            txt_info = parsed_txt_data.get(t, {})
            banco = txt_info.get('VOUCHER_PSP', 'COMPLETAR_BANCO')
            nro_op = txt_info.get('VOUCHER_Operacion_PSP', 'COMPLETAR_OPERACION')
            voucher_fecha = txt_info.get('VOUCHER_FECHA', 'COMPLETAR_FECHA')
            
            if d:
                empresa = d.get("creditor", {}).get("name", "N/A")
                
                activity_list = d.get("activity_list", [])
                estado = activity_list[0].get("name", "N/A") if (isinstance(activity_list, list) and len(activity_list) > 0) else "N/A"
                
                public_id = d.get("public_id", "N/A")
                pen = d.get("sub_total", {}).get("currency", "N/A")
                
                try: monto_voucher = float(d.get("sub_total", {}).get("value", 0.0))
                except: monto_voucher = 0.0
                    
                try: monto_kashio = float(d.get("total", {}).get("value", 0.0))
                except: monto_kashio = 0.0
                    
                balance = monto_voucher - monto_kashio
                
                row = {
                    "Tipo": "Reg.Interna",
                    "Tipo2": "EECC",
                    "Empresa": empresa,
                    "Fecha de revisión": fecha_revision,
                    "Mes": mes_revision,
                    "PSP_TIN": t,
                    "PSP_TIN concatenado": f"'{t}',",
                    "Estado": estado,
                    "Public ID": public_id,
                    "inv_id concatenado": f"'{public_id}',",
                    "PEN": pen,
                    "Monto voucher": monto_voucher,
                    "Monto Kashio": monto_kashio,
                    "Balance": balance,
                    "CANAL": "WEB",
                    "Banco": banco,
                    "Nro OP": nro_op,
                    "VOUCHER_FECHA": voucher_fecha
                }
                tabla_rows.append(row)
                
            else:
                row = {
                    "Tipo": "Reg.Interna",
                    "Tipo2": "EECC",
                    "Empresa": "ERROR EN CONSULTA",
                    "Fecha de revisión": fecha_revision,
                    "Mes": mes_revision,
                    "PSP_TIN": t,
                    "PSP_TIN concatenado": f"'{t}',",
                    "Estado": f"Error HTTP {res.get('error')}",
                    "Public ID": "N/A",
                    "inv_id concatenado": "N/A",
                    "PEN": "N/A",
                    "Monto voucher": 0.0,
                    "Monto Kashio": 0.0,
                    "Balance": 0.0,
                    "CANAL": "WEB",
                    "Banco": banco,
                    "Nro OP": nro_op,
                    "VOUCHER_FECHA": voucher_fecha
                }
                tabla_rows.append(row)
        
        if tabla_rows:
            df_tabla = pd.DataFrame(tabla_rows)
            st.markdown("### 📋 Tabla de Conciliación (Editable)")
            st.caption("💡 **Para eliminar una fila:** Selecciona la casilla a la izquierda de la fila y presiona la tecla 'Suprimir' (Delete), o usa el ícono de la papelera en la esquina superior derecha de la tabla.")
            
            # Tabla interactiva (permite eliminar filas)
            df_editado = st.data_editor(
                df_tabla, 
                num_rows="dynamic", 
                use_container_width=True, 
                key="editor_tab3"
            )
            
            # Generación del archivo Excel en memoria
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_editado.to_excel(writer, index=False, sheet_name='Conciliacion')
            
            st.download_button(
                label="📥 Descargar Tabla en Excel (.xlsx)",
                data=excel_buffer.getvalue(),
                file_name=f"Conciliacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            # Recálculo dinámico de la trama basado en la tabla editada
            trama_dinamica = []
            for _, row in df_editado.iterrows():
                trama_line = f"{{'VOUCHER_PSP':'{row['Banco']}','VOUCHER_PSP_TIN': '{row['PSP_TIN']}','VOUCHER_Currency': '{row['PEN']}','VOUCHER_Amount': {row['Monto voucher']},'VOUCHER_Operacion_PSP': '{row['Nro OP']}','VOUCHER_FECHA':'{row['VOUCHER_FECHA']}'}},"
                trama_dinamica.append(trama_line)
                
            st.markdown("### 🔗 Trama de salida para la Pestaña 2")
            trama_completa_str = "\n".join(trama_dinamica)
            st.text_area("Copia estas líneas y pégalas directamente dentro del cuadro de texto de la Pestaña 2:", value=trama_completa_str, height=180)
