# -*- coding: utf-8 -*-
"""
Aplicación Streamlit de procesamiento de pagos KashIO.
Versión corporativa optimizada - Flujo unificado.
"""

import streamlit as st
import requests
import pandas as pd
import ast
import re
from datetime import datetime
from io import BytesIO

# ==========================================
# 1. CONFIGURACION GLOBAL Y ESTADOS
# ==========================================
st.set_page_config(page_title="Procesador KashIO", layout="wide")

SERVICE_URL = "https://t5jezcpiwc.execute-api.us-east-1.amazonaws.com/LIVE"
HEADERS = {'content-type': 'application/json'}
AUTH_USER = ""
AUTH_PSW = ""

DATA_PSP = [
    {'PSP': 'BCP','Currency': 'PEN','ACC': 'acc_R8eMfP7Cdaq5ScUavLXBMX'},
    {'PSP': 'BCP','Currency': 'USD','ACC': 'acc_ddcdxWdBFhtwhWMJQKZ46h'},
    {'PSP': 'BBVA','Currency': 'PEN','ACC': 'acc_cZpQmjzkKdoEvmWfj4Qm9K'},
    {'PSP': 'BBVA','Currency': 'USD','ACC': 'acc_S6tfrAUEnLcH2VNDiGt9d9'},
    {'PSP': 'IBK','Currency': 'PEN','ACC': 'acc_rbzSXQuvc5FfMbR2z4vMGd'},
    {'PSP': 'IBK','Currency': 'USD','ACC': 'acc_9FmSeoNUiQTL7bHd2JzJCb'},
    {'PSP': 'SCOTIA','Currency': 'PEN','ACC': 'acc_R8eMfP7Cdaq5ScUavLXBMS'},
    {'PSP': 'SCOTIA','Currency': 'USD','ACC': 'acc_cUtpw5swumqQRZDRZEcy23'},
    {'PSP': 'KASNET','Currency': 'PEN','ACC': 'acc_5w9ujngtRTf8qgG7xYyMJm'},
    {'PSP': 'BILLETERA-NIUBIZ','Currency': 'PEN','ACC': 'acc_KCKUEA8gNiAk9r73zz6NMh'},
    {'PSP': 'CARD','Currency': 'PEN','ACC': 'acc_8v3EMCq7EuuK2Czp54KF6e'},
    {'PSP': 'BILLETERA-GMONEY','Currency': 'PEN','ACC': 'acc_24205fe371034deb9731'}
]
DICT_ACC = {(row['PSP'], row['Currency']): row['ACC'] for row in DATA_PSP}

def inicializar_estados():
    """Centraliza la declaración de variables globales de interfaz."""
    if "df_conciliacion" not in st.session_state:
        st.session_state.df_conciliacion = pd.DataFrame()
    if "trama_generada" not in st.session_state:
        st.session_state.trama_generada = ""
    if "alertas_pagados" not in st.session_state:
        st.session_state.alertas_pagados = []

inicializar_estados()

# ==========================================
# 2. LOGICA DE NEGOCIO Y RED (CORE)
# ==========================================

def consultar_api_tins(tin_list):
    """Módulo centralizado de consultas GET."""
    resultados = []
    with requests.Session() as session:
        session.auth = (AUTH_USER, AUTH_PSW)
        session.headers.update(HEADERS)
        session.headers.update({'User-Agent': 'PostmanRuntime/7.26.8'})
        
        progreso = st.progress(0)
        for idx, tin in enumerate(tin_list):
            try:
                r = session.get(f"{SERVICE_URL}/consultar/{tin}?search_by=PSP_TIN", timeout=15)
                data = r.json() if r.status_code in (200, 201) else None
                resultados.append({"tin": tin, "data": data, "error": None if data else r.status_code})
            except Exception as e:
                resultados.append({"tin": tin, "data": None, "error": str(e)})
            progreso.progress((idx + 1) / len(tin_list))
            
        progreso.empty()
    return resultados

def ejecutar_post_pagos(payload_list, usuario_operacion):
    """Módulo centralizado de ejecución POST automatizada y manual."""
    url_pago = f"{SERVICE_URL}/pagomanual"
    resultados = []
    
    with requests.Session() as session:
        session.auth = (AUTH_USER, AUTH_PSW)
        session.headers.update(HEADERS)
        
        progreso = st.progress(0)
        for index, item in enumerate(payload_list):
            tin = item.get('VOUCHER_PSP_TIN', 'N/A')
            acc = DICT_ACC.get((item.get('VOUCHER_PSP'), item.get('VOUCHER_Currency')))
            res_row = {'TIN': tin, 'STATUS': 'FALTA_CUENTA', 'MENSAJE': 'PSP/Moneda no configurado'}
            
            if acc:
                payload = {
                    "invoice": {"id": tin},
                    "payer": {"reference_number": tin},
                    "amount": {"value": float(item.get('VOUCHER_Amount', 0)), "currency": item.get('VOUCHER_Currency')},
                    "psp_account": {"id": acc},
                    "operation_no": item.get('VOUCHER_Operacion_PSP'),
                    "operation_date": item.get('VOUCHER_FECHA'),
                    "branch_code": "",
                    "channel_code": "WEB",
                    "force_expire_payment": True,
                    "metadata": {"code": usuario_operacion, "clave": "AR"} 
                }
                try:
                    resp = session.post(url_pago, timeout=30, json=payload)
                    res_row['STATUS'] = resp.status_code
                    res_row['MENSAJE'] = 'OK' if resp.status_code == 200 else resp.text
                except Exception as e:
                    res_row['STATUS'] = 'ERROR_RED'
                    res_row['MENSAJE'] = str(e)
            
            resultados.append(res_row)
            progreso.progress((index + 1) / len(payload_list))
            
        progreso.empty()
    return pd.DataFrame(resultados)

def procesar_archivo_bancario(file_content, filename):
    """Procesador lineal y limpio de archivos TXT. Evita anidamiento profundo."""
    raw_text = file_content.decode("latin-1", errors="replace")
    clean_text = re.sub(r'[^\x20-\x7E\r\n]', ' ', raw_text)
    lines = clean_text.splitlines()
    
    if not lines: return {}
    
    banco, moneda = "DESCONOCIDO", "PEN"
    first = lines[0]
    if first.startswith("0120"): banco, moneda = "BBVA", "USD" if "USD" in first[16:25] else "PEN"
    elif first.startswith("0791501"): banco, moneda = "IBK", "PEN"
    elif first.startswith("0791502"): banco, moneda = "IBK", "USD"
    elif first.startswith("CC"): banco, moneda = "BCP", "USD" if "1941" in first[0:15] or "DOLARES" in filename.upper() else "PEN"

    parsed_data = {}
    for line in lines:
        tin, date_str, amount_str, op_str = None, None, None, None
        
        if banco == "IBK" and line.startswith(("0791501", "0791502")) and len(line) >= 149:
            tin, date_str, amount_str, op_str = line[37:49], line[82:90], line[96:109], line[141:149]
        elif banco == "BBVA" and line.startswith("02") and len(line) >= 140:
            tin, date_str, amount_str, op_str = line[48:60], line[135:143], line[80:95], line[70:80]
        elif banco == "BCP" and line.startswith("DD") and len(line) >= 150:
            tin, date_str, amount_str, op_str = line[15:27], line[57:65], line[73:88], line[143:149]
            
        if tin:
            tin_c = re.sub(r'[^\d]', '', tin)
            date_c = re.sub(r'[^\d]', '', date_str)
            amount_c = re.sub(r'[^\d]', '', amount_str)
            
            if tin_c and date_c and amount_c:
                try:
                    dt = datetime.strptime(date_c, "%Y%m%d")
                    parsed_data[tin_c] = {
                        'VOUCHER_PSP': banco, 
                        'VOUCHER_Currency': moneda,
                        'VOUCHER_Amount': float(amount_c) / 100.0,
                        'VOUCHER_Operacion_PSP': op_str.strip(),
                        'VOUCHER_FECHA': str((dt - datetime(1899, 12, 30)).days)
                    }
                except ValueError: pass
                
    return parsed_data

def consolidar_datos_tabla(resultados_api, datos_txt):
    """Transforma los datos crudos en la estructura requerida para la tabla final."""
    filas = []
    pagados = []
    fecha_rev = datetime.now().strftime("%d/%m/%Y")
    mes_rev = datetime.now().strftime("%B")
    
    for res in resultados_api:
        t = res["tin"]
        d = res["data"]
        
        info_txt = datos_txt.get(t, {})
        banco = info_txt.get('VOUCHER_PSP', 'COMPLETAR_BANCO')
        nro_op = info_txt.get('VOUCHER_Operacion_PSP', 'COMPLETAR_OPERACION')
        voucher_fecha = info_txt.get('VOUCHER_FECHA', 'COMPLETAR_FECHA')
        
        if d:
            act_list = d.get("activity_list", [])
            estado = act_list[0].get("name", "N/A") if (isinstance(act_list, list) and act_list) else "N/A"
            
            if d.get("status") == "PAID":
                pagados.append(t)
            
            monto_voucher = float(d.get("sub_total", {}).get("value", 0.0)) if d.get("sub_total") else 0.0
            monto_kashio = float(d.get("total", {}).get("value", 0.0)) if d.get("total") else 0.0
            
            filas.append({
                "Tipo": "Reg.Interna", "Tipo2": "EECC",
                "Empresa": d.get("creditor", {}).get("name", "N/A"),
                "Fecha de revision": fecha_rev, "Mes": mes_rev,
                "PSP_TIN": t, "PSP_TIN concatenado": f"'{t}',",
                "Estado": estado,
                "Public ID": d.get("public_id", "N/A"),
                "inv_id concatenado": f"'{d.get('public_id', 'N/A')}',",
                "PEN": d.get("sub_total", {}).get("currency", "N/A"),
                "Monto voucher": monto_voucher, "Monto Kashio": monto_kashio,
                "Balance": monto_voucher - monto_kashio,
                "CANAL": "WEB", "Banco": banco, "Nro OP": nro_op, "VOUCHER_FECHA": voucher_fecha
            })
        else:
            filas.append({
                "Tipo": "Reg.Interna", "Tipo2": "EECC", "Empresa": "ERROR EN CONSULTA",
                "Fecha de revision": fecha_rev, "Mes": mes_rev,
                "PSP_TIN": t, "PSP_TIN concatenado": f"'{t}',",
                "Estado": f"Error HTTP {res.get('error')}",
                "Public ID": "N/A", "inv_id concatenado": "N/A", "PEN": "N/A",
                "Monto voucher": 0.0, "Monto Kashio": 0.0, "Balance": 0.0,
                "CANAL": "WEB", "Banco": banco, "Nro OP": nro_op, "VOUCHER_FECHA": voucher_fecha
            })
            
    return pd.DataFrame(filas), pagados

def extraer_trama_desde_df(df):
    """Construye el texto plano de la trama iterando la tabla editada."""
    lineas = []
    for _, r in df.iterrows():
        lineas.append(f"{{'VOUCHER_PSP':'{r['Banco']}','VOUCHER_PSP_TIN': '{r['PSP_TIN']}','VOUCHER_Currency': '{r['PEN']}','VOUCHER_Amount': {r['Monto voucher']},'VOUCHER_Operacion_PSP': '{r['Nro OP']}','VOUCHER_FECHA':'{r['VOUCHER_FECHA']}'}},")
    return "\n".join(lineas)

def evaluar_trama_texto(texto):
    """Convierte de forma segura el texto crudo a lista de diccionarios."""
    clean = texto.strip()
    if not clean: return []
    if not clean.startswith("["): clean = f"[{clean}]"
    return ast.literal_eval(clean)

# ==========================================
# 3. INTERFAZ DE USUARIO (PRESENTACION)
# ==========================================

st.title("Procesamiento de Pagos KashIO")
st.markdown("Plataforma operativa para validación y regularización de transacciones.")

# ------------------------------------------
# SECCION 1: CONSULTA DE DATOS
# ------------------------------------------
st.subheader("1. Consulta y Procesamiento Automático")

col_a, col_b = st.columns([1, 1])
with col_a:
    input_tins = st.text_area("Códigos TIN", placeholder="Ingrese los códigos separados por salto de línea")
with col_b:
    archivo_txt = st.file_uploader("Archivo bancario (Opcional)", type=['txt'])

if st.button("Ejecutar Consulta", type="primary"):
    candidatos = re.findall(r'\d+', input_tins)
    tins_validos = [t[2:] if (t.startswith("00") and len(t)>12) else t for t in candidatos if len(t)==12 or (t.startswith("00") and len(t[2:])==12)]
    
    if not tins_validos:
        st.warning("No se identificaron códigos TIN con la longitud requerida (12 dígitos).")
    else:
        lista_unica = list(dict.fromkeys(tins_validos))
        datos_txt = procesar_archivo_bancario(archivo_txt.getvalue(), archivo_txt.name) if archivo_txt else {}
        
        with st.spinner("Conectando al sistema central..."):
            res_api = consultar_api_tins(lista_unica)
            df_final, pagados = consolidar_datos_tabla(res_api, datos_txt)
            
            st.session_state.df_conciliacion = df_final
            st.session_state.alertas_pagados = pagados

# ------------------------------------------
# SECCION 2: RESULTADOS Y CONCILIACION
# ------------------------------------------
if not st.session_state.df_conciliacion.empty:
    st.divider()
    st.subheader("2. Tabla de Conciliación")
    
    if st.session_state.alertas_pagados:
        st.warning(f"Se identificaron {len(st.session_state.alertas_pagados)} operaciones con estado previo de liquidación (PAID): {', '.join(st.session_state.alertas_pagados)}")

    df_editado = st.data_editor(st.session_state.df_conciliacion, num_rows="dynamic", use_container_width=True)
    
    # Sincronización en vivo
    trama_texto_vivo = extraer_trama_desde_df(df_editado)
    st.session_state.trama_generada = trama_texto_vivo
    
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        df_editado.to_excel(writer, index=False, sheet_name='Conciliacion')
    st.download_button("Descargar Reporte (Excel)", excel_buffer.getvalue(), f"Reporte_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ------------------------------------------
    # SECCION 3: EJECUCION DE OPERACIONES
    # ------------------------------------------
    st.divider()
    st.subheader("3. Panel de Ejecución")
    
    usuario_operador = st.text_input("Iniciales del Usuario Operativo", value="KNC")
    
    if st.button("Ejecutar Operaciones Automáticamente", type="primary"):
        payload_auto = evaluar_trama_texto(trama_texto_vivo)
        if not payload_auto:
            st.error("La tabla de operaciones se encuentra vacía.")
        else:
            with st.spinner("Procesando operaciones automáticas..."):
                df_resultados = ejecutar_post_pagos(payload_auto, usuario_operador)
                st.dataframe(df_resultados, use_container_width=True)
                st.success("Flujo automático completado.")

    st.markdown("---")
    st.markdown("Estructura de Datos Manual (Trama)")
    trama_ingreso = st.text_area("Caja de edición técnica", value=st.session_state.trama_generada, height=180)
    
    if st.button("Ejecutar Trama Manual"):
        try:
            payload_manual = evaluar_trama_texto(trama_ingreso)
            if not payload_manual: raise ValueError("Estructura vacía.")
            with st.spinner("Procesando operaciones manuales..."):
                df_resultados = ejecutar_post_pagos(payload_manual, usuario_operador)
                st.dataframe(df_resultados, use_container_width=True)
                st.success("Flujo manual completado.")
        except Exception as e:
            st.error(f"Error de validación estructural: {e}")
