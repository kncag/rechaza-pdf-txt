# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import ast
import re
from datetime import datetime
from io import BytesIO

st.set_page_config(page_title="Procesador KashIO", layout="wide")

SERVICE_URL = "https://t5jezcpiwc.execute-api.us-east-1.amazonaws.com/LIVE"
HEADERS = {'content-type': 'application/json'}
AUTH_USER = ""
AUTH_PSW = ""

DICT_ACC = {
    ('BCP',              'PEN'): 'acc_R8eMfP7Cdaq5ScUavLXBMX',
    ('BCP',              'USD'): 'acc_ddcdxWdBFhtwhWMJQKZ46h',
    ('BBVA',             'PEN'): 'acc_cZpQmjzkKdoEvmWfj4Qm9K',
    ('BBVA',             'USD'): 'acc_S6tfrAUEnLcH2VNDiGt9d9',
    ('IBK',              'PEN'): 'acc_rbzSXQuvc5FfMbR2z4vMGd',
    ('IBK',              'USD'): 'acc_9FmSeoNUiQTL7bHd2JzJCb',
    ('SCOTIA',           'PEN'): 'acc_R8eMfP7Cdaq5ScUavLXBMS',
    ('SCOTIA',           'USD'): 'acc_cUtpw5swumqQRZDRZEcy23',
    ('KASNET',           'PEN'): 'acc_5w9ujngtRTf8qgG7xYyMJm',
    ('BILLETERA-NIUBIZ', 'PEN'): 'acc_KCKUEA8gNiAk9r73zz6NMh',
    ('CARD',             'PEN'): 'acc_8v3EMCq7EuuK2Czp54KF6e',
    ('BILLETERA-GMONEY', 'PEN'): 'acc_24205fe371034deb9731',
}

for key in ["df_conciliacion", "trama_generada", "alertas_pagados", "raw_api_results", "lineas_no_leidas"]:
    if key not in st.session_state:
        st.session_state[key] = pd.DataFrame() if key == "df_conciliacion" else ([] if key != "trama_generada" else "")


def consultar_api_tins(tin_list):
    resultados = []
    with requests.Session() as session:
        session.auth = (AUTH_USER, AUTH_PSW)
        session.headers.update({**HEADERS, 'User-Agent': 'PostmanRuntime/7.26.8'})
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
                    resp = session.post(f"{SERVICE_URL}/pagomanual", timeout=30, json=payload)
                    res_row['STATUS'] = resp.status_code
                    res_row['MENSAJE'] = 'OK' if resp.status_code == 200 else resp.text
                except Exception as e:
                    res_row['STATUS'] = 'ERROR_RED'
                    res_row['MENSAJE'] = str(e)
            resultados.append(res_row)
            progreso.progress((index + 1) / len(payload_list))
        progreso.empty()
    return pd.DataFrame(resultados)


def _fecha_serial(yyyymmdd):
    dt = datetime.strptime(yyyymmdd, "%Y%m%d")
    return str((dt - datetime(1899, 12, 30)).days)


def _parsear_bcp(line):
    # TIN: 12 dígitos tras el último " 00000000". Bloque de 73 dígitos antes de "EFECTIVO":
    # FECHA1[0:8] FECHA2[8:16] ... OPERACION[61:73] (los últimos 6 son el Nro OP).
    tins = re.findall(r' 0{8}(\d{12})', line)
    blk = re.search(r'(\d{73})EFECTIVO', line)
    if not tins or not blk:
        return None
    b = blk.group(1)
    return {
        'tin': tins[-1],
        'VOUCHER_Operacion_PSP': b[67:73],
        'VOUCHER_FECHA': _fecha_serial(b[0:8]),
    }


def _parsear_ibk(line):
    # TIN: 12 dígitos delimitados por espacios. Fecha: primera "2026xxxx".
    # Operación/Referencia: 8 dígitos tras la última fecha "2026xxxx" de la línea.
    mt = re.search(r'\s(\d{12})\s', line)
    fechas = list(re.finditer(r'2026\d{4}', line))
    if not mt or not fechas:
        return None
    ref = line[fechas[-1].end():fechas[-1].end() + 8]
    if not re.fullmatch(r'\d{8}', ref):
        return None
    return {
        'tin': mt.group(1),
        'VOUCHER_Operacion_PSP': ref.lstrip('0'),
        'VOUCHER_FECHA': _fecha_serial(fechas[0].group()),
    }


def _parsear_bbva(line):
    # TIN: 12 dígitos antes del código de servicio "084x". Cuerpo de 81 chars desde "084x":
    # REFERENCIA[54:60] FECHA[65:73] CANAL[73:81].
    m = re.search(r'(\d{12})\s+(084\d.{76})', line)
    if not m:
        return None
    tin, b = m.group(1), m.group(2)
    return {
        'tin': tin,
        'VOUCHER_Operacion_PSP': b[54:60].lstrip('0'),
        'VOUCHER_FECHA': _fecha_serial(b[65:73]),
    }


def procesar_archivo_bancario(file_content):
    lines = re.sub(r'[^\x20-\x7E\r\n]', ' ', file_content.decode("latin-1", errors="replace")).splitlines()
    if not lines:
        return {}, []

    banco = "DESCONOCIDO"
    first = lines[0]
    if first.startswith("0120"):
        banco = "BBVA"
    elif first.startswith(("0791501", "0791502")):
        banco = "IBK"
    elif first.startswith("CC"):
        banco = "BCP"

    parsers = {"BCP": _parsear_bcp, "IBK": _parsear_ibk, "BBVA": _parsear_bbva}
    detalle = {"BCP": "DD", "IBK": ("0791501", "0791502"), "BBVA": "02"}.get(banco)
    parser = parsers.get(banco)

    parsed_data = {}
    no_leidas = []
    if not parser:
        return parsed_data, no_leidas

    for line in lines:
        if not line.startswith(detalle):
            continue
        try:
            campos = parser(line)
            if campos is None:
                raise ValueError("estructura no reconocida")
            tin = campos.pop('tin')
            parsed_data[tin] = {'VOUCHER_PSP': banco, **campos}
        except Exception:
            no_leidas.append(line.rstrip())

    return parsed_data, no_leidas


def consolidar_datos_tabla(resultados_api, datos_txt):
    filas = []
    pagados = []
    ahora = datetime.now()
    fecha_rev = f"{ahora.month}/{ahora.day}/{ahora.year}"
    mes_rev = ahora.strftime("%B")

    COLUMN_ORDER = [
        "Tipo", "Tipo2", "Empresa", "Fecha de revision", "Mes",
        "PSP_TIN", "PSP_TIN concatenado", "Estado", "Public ID",
        "inv_id concatenado", "PEN", "Monto voucher", "Monto Kashio",
        "Balance", "CANAL", "Banco", "Nro OP", "VOUCHER_FECHA"
    ]

    for res in resultados_api:
        t, d = res["tin"], res["data"]
        info_txt = datos_txt.get(t, {})
        base = {
            "Tipo": "Reg.Interna", "Tipo2": "EECC",
            "Fecha de revision": fecha_rev, "Mes": mes_rev,
            "PSP_TIN": t, "PSP_TIN concatenado": f"'{t}',",
            "CANAL": "WEB",
            "Banco": info_txt.get('VOUCHER_PSP', 'COMPLETAR_BANCO'),
            "Nro OP": info_txt.get('VOUCHER_Operacion_PSP', 'COMPLETAR_OPERACION'),
            "VOUCHER_FECHA": info_txt.get('VOUCHER_FECHA', 'COMPLETAR_FECHA'),
        }
        if d:
            act_list = d.get("activity_list", [])
            estado = act_list[0].get("name", "N/A") if (isinstance(act_list, list) and act_list) else "N/A"
            if d.get("status") == "PAID":
                pagados.append(t)
            monto_voucher = float(d.get("sub_total", {}).get("value", 0.0)) if d.get("sub_total") else 0.0
            monto_kashio = float(d.get("total", {}).get("value", 0.0)) if d.get("total") else 0.0
            filas.append({**base,
                "Empresa": d.get("creditor", {}).get("name", "N/A"),
                "Estado": estado,
                "Public ID": d.get("public_id", "N/A"),
                "inv_id concatenado": f"'{d.get('public_id', 'N/A')}',",
                "PEN": d.get("sub_total", {}).get("currency", "N/A"),
                "Monto voucher": monto_voucher, "Monto Kashio": monto_kashio,
                "Balance": monto_voucher - monto_kashio,
            })
        else:
            filas.append({**base,
                "Empresa": "ERROR EN CONSULTA",
                "Estado": f"Error HTTP {res.get('error')}",
                "Public ID": "N/A", "inv_id concatenado": "N/A", "PEN": "N/A",
                "Monto voucher": 0.0, "Monto Kashio": 0.0, "Balance": 0.0,
            })

    return pd.DataFrame(filas)[COLUMN_ORDER], pagados


def extraer_trama_desde_df(df):
    return "\n".join(
        f"{{'VOUCHER_PSP':'{r['Banco']}','VOUCHER_PSP_TIN': '{r['PSP_TIN']}','VOUCHER_Currency': '{r['PEN']}','VOUCHER_Amount': {r['Monto voucher']},'VOUCHER_Operacion_PSP': '{r['Nro OP']}','VOUCHER_FECHA':'{r['VOUCHER_FECHA']}'}},"
        for _, r in df.iterrows()
    )


def evaluar_trama_texto(texto):
    clean = texto.strip()
    if not clean:
        return []
    if not clean.startswith("["):
        clean = f"[{clean}]"
    return ast.literal_eval(clean)


def aplicar_alertas_tabla(row):
    estilos = [''] * len(row)
    ALERTA = 'background-color: #ffe6e6; color: #cc0000; font-weight: bold;'
    if 'Balance' in row.index:
        try:
            if abs(float(row['Balance'])) > 5:
                estilos[row.index.get_loc('Balance')] = ALERTA
        except: pass
    for col in ['Monto voucher', 'Monto Kashio']:
        if col in row.index:
            try:
                if float(row[col]) >= 500:
                    estilos[row.index.get_loc(col)] = ALERTA
            except: pass
    return estilos


# ==========================================
# UI
# ==========================================
st.title("Procesamiento de Pagos KashIO")
st.markdown("Plataforma operativa para validación y regularización de transacciones.")
st.subheader("1. Consulta y Procesamiento Automático")

col_a, col_b = st.columns([1, 1])
with col_a:
    input_tins = st.text_area("Códigos TIN", placeholder="Ingrese los códigos separados por salto de línea")
with col_b:
    archivo_txt = st.file_uploader("Archivo bancario (Opcional)", type=['txt'])

col_btn1, col_btn2, col_spacer = st.columns([1.5, 1.5, 7])
with col_btn1:
    btn_consulta = st.button("Ejecutar Consulta", type="primary", width='stretch')
with col_btn2:
    btn_json = st.button("Revisar Respuestas JSON", type="secondary", width='stretch')

if btn_consulta:
    candidatos = re.findall(r'\d+', input_tins)
    tins_validos = [t[2:] if (t.startswith("00") and len(t) > 12) else t for t in candidatos if len(t) == 12 or (t.startswith("00") and len(t[2:]) == 12)]
    if not tins_validos:
        st.warning("No se identificaron códigos TIN con la longitud requerida (12 dígitos).")
    else:
        lista_unica = list(dict.fromkeys(tins_validos))
        if archivo_txt:
            datos_txt, lineas_no_leidas = procesar_archivo_bancario(archivo_txt.getvalue())
        else:
            datos_txt, lineas_no_leidas = {}, []
        with st.spinner("Conectando al sistema central..."):
            res_api = consultar_api_tins(lista_unica)
            df_final, pagados = consolidar_datos_tabla(res_api, datos_txt)
            st.session_state.df_conciliacion = df_final
            st.session_state.alertas_pagados = pagados
            st.session_state.raw_api_results = res_api
            st.session_state.lineas_no_leidas = lineas_no_leidas

if btn_json:
    if not st.session_state.raw_api_results:
        st.info("Debe ejecutar una consulta previa para cargar los registros en memoria.")
    else:
        st.markdown("### Respuestas Crudas del Servidor")
        for registro in st.session_state.raw_api_results:
            with st.expander(f"Código TIN: {registro['tin']}", expanded=False):
                if registro['data']:
                    st.json(registro['data'])
                else:
                    st.error(f"Sin datos legibles. Código de error HTTP o Red: {registro['error']}")

if not st.session_state.df_conciliacion.empty:
    st.divider()
    st.subheader("2. Tabla de Conciliación")
    if st.session_state.alertas_pagados:
        st.warning(f"Se identificaron {len(st.session_state.alertas_pagados)} operaciones con estado previo de liquidación (PAID): {', '.join(st.session_state.alertas_pagados)}")

    if st.session_state.lineas_no_leidas:
        n = len(st.session_state.lineas_no_leidas)
        with st.expander(f"⚠️ {n} línea(s) del archivo bancario no pudieron leerse — revisar manualmente", expanded=True):
            st.caption("Estas líneas no coincidieron con la estructura esperada del banco y NO se incluyeron en la conciliación.")
            for ln in st.session_state.lineas_no_leidas:
                st.code(ln, language=None)

    df_estilizado = st.session_state.df_conciliacion.style.apply(aplicar_alertas_tabla, axis=1)
    df_editado = st.data_editor(df_estilizado, num_rows="dynamic", width='stretch')

    montos = pd.to_numeric(df_editado.get("Monto voucher"), errors="coerce")
    montos_k = pd.to_numeric(df_editado.get("Monto Kashio"), errors="coerce")
    balances = pd.to_numeric(df_editado.get("Balance"), errors="coerce")
    n_monto = int(((montos > 500) | (montos_k > 500)).sum())
    n_balance = int((balances.abs() > 5).sum())
    if n_monto or n_balance:
        avisos = []
        if n_monto:
            avisos.append(f"{n_monto} operación(es) con Monto voucher o Monto Kashio mayor a 500")
        if n_balance:
            avisos.append(f"{n_balance} operación(es) con Balance mayor a 5")
        st.error("⚠️ " + " · ".join(avisos) + ". Revisar antes de ejecutar.")

    trama_texto_vivo = extraer_trama_desde_df(df_editado)
    st.session_state.trama_generada = trama_texto_vivo

    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        df_editado.to_excel(writer, index=False, sheet_name='Conciliacion')
    st.download_button("Descargar Reporte (Excel)", excel_buffer.getvalue(), f"Reporte_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

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
                st.dataframe(df_resultados, width='stretch')
                st.success("Flujo automático completado.")

    st.markdown("---")
    st.markdown("Estructura de Datos Manual (Trama)")
    trama_ingreso = st.text_area("Caja de edición técnica", value=st.session_state.trama_generada, height=180)

    if st.button("Ejecutar Trama Manual"):
        try:
            payload_manual = evaluar_trama_texto(trama_ingreso)
            if not payload_manual:
                raise ValueError("Estructura vacía.")
            with st.spinner("Procesando operaciones manuales..."):
                df_resultados = ejecutar_post_pagos(payload_manual, usuario_operador)
                st.dataframe(df_resultados, width='stretch')
                st.success("Flujo manual completado.")
        except Exception as e:
            st.error(f"Error de validación estructural: {e}")
