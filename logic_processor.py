# logic_processor.py
import os
import re
import tempfile
import requests
import pandas as pd
from datetime import datetime, timedelta, time, date
from pathlib import Path
from imap_tools import MailBox, AND

# --- Configuración ---
ENDPOINTS = {
    "udep": {
        "subir": "https://rx06her9g5.execute-api.us-east-1.amazonaws.com/UDEP/subir",
        "procesar": "https://rx06her9g5.execute-api.us-east-1.amazonaws.com/UDEP/procesar",
        "sincronizar": "https://rx06her9g5.execute-api.us-east-1.amazonaws.com/UDEP/sincronizar",
        "reconciliar": "https://rx06her9g5.execute-api.us-east-1.amazonaws.com/UDEP/reconciliar",
    },
    "euro": {
        "subir": "https://6dsz2xtbx4.execute-api.us-east-1.amazonaws.com/EUROMOTORS/subir",
        "procesar": "https://6dsz2xtbx4.execute-api.us-east-1.amazonaws.com/EUROMOTORS/procesar",
        "sincronizar": "https://6dsz2xtbx4.execute-api.us-east-1.amazonaws.com/EUROMOTORS/sincronizar",
        "reconciliar": "https://6dsz2xtbx4.execute-api.us-east-1.amazonaws.com/EUROMOTORS/reconciliar",
    }
}

# Reglas de clasificación (Simplificadas para el ejemplo)
RULES_EURO = [
    ("sub_YK5GU0000019", ["bws", "sbp"], []),
    ("sub_YK5GU0000020", ["cdpg"], ["dolares"]),
    ("sub_YK5GU0000022", ["dolares"], []),
    ("sub_YK5GU0000021", ["eur"], [])
]
RULES_UDEP = [
    ("sub_YK5GU0000025", ["cdpg"], []),
    ("sub_YK5GU0000022", ["rec"], []),
    ("sub_YK5GU0000023", ["bws", "sbp"], []),
    ("sub_YK5GU0000024", ["2103093"], [])
]

def conectar_imap(user, password):
    """Prueba la conexión al correo."""
    try:
        mailbox = MailBox('outlook.office365.com').login(user, password)
        return mailbox
    except Exception as e:
        raise Exception(f"Error de autenticación IMAP: {e}")

def descargar_adjuntos(mailbox, fecha_objetivo, carpetas_filtro):
    """
    Descarga adjuntos usando la lógica de ventana 19:00 - 19:00.
    Retorna una lista de diccionarios: {'filename', 'content', 'folder', 'date'}
    """
    adjuntos_encontrados = []
    
    # Definir ventana de tiempo
    fin = datetime.combine(fecha_objetivo, time(19, 0))
    inicio = fin - timedelta(days=1)
    
    # Buscar correos en el rango de fechas (IMAP busca por día completo, filtramos después)
    criteria = AND(date_gte=inicio.date(), date_lt=fecha_objetivo + timedelta(days=1))
    
    for msg in mailbox.fetch(criteria):
        # Filtro de hora preciso en Python
        # msg.date suele tener timezone, lo normalizamos
        msg_date = msg.date.replace(tzinfo=None)
        
        if not (inicio <= msg_date <= fin):
            continue

        # Filtro de carpeta (aproximado, ya que IMAP 'INBOX' es global, 
        # a menos que busquemos en carpetas especificas. Aquí asumimos INBOX y filtramos por subject/remitente si fuera necesario
        # Ojo: mailbox.folder.set() se usa para cambiar carpetas en IMAP.
        # Para simplificar, asumimos que están en INBOX o iteramos carpetas afuera.
        pass 

        for att in msg.attachments:
            if att.filename.lower().endswith(".txt") and "crep" not in att.filename.lower():
                adjuntos_encontrados.append({
                    "filename": att.filename,
                    "content": att.payload, # Bytes del archivo
                    "email_date": msg_date
                })
                
    return adjuntos_encontrados

def match_subscription(fname, rules):
    name = Path(fname).stem.lower()
    for sub_id, include, exclude in rules:
        if any(inc.lower() in name for inc in include) and not any(exc.lower() in name for exc in exclude):
            return sub_id
    return None

def api_upload_flow(file_bytes, filename, sub_id, flow_type):
    """Ejecuta el flujo de subida a la API (Subir -> Procesar -> Sincronizar -> Reconciliar)."""
    eps = ENDPOINTS[flow_type]
    
    # 1. SUBIR
    files = {"edt": (filename, file_bytes)}
    data = {"subscription_public_id": sub_id}
    try:
        r = requests.post(eps["subir"], files=files, data=data)
        r.raise_for_status()
    except Exception as e:
        return {"status": "Error Subida", "details": str(e)}

    # 2. PROCESAR
    try:
        requests.post(eps["procesar"]).raise_for_status()
    except Exception as e:
        return {"status": "Error Procesar", "details": str(e)}
        
    # 3. SINCRONIZAR (Simplificado sin el loop complejo para el ejemplo, pero funcional)
    try:
        r = requests.post(eps["sincronizar"])
        sync_data = r.json()
        # Normalizar respuesta
        if isinstance(sync_data, list) and sync_data: sync_data = sync_data[0]
        proc = sync_data.get("processed_record", 0)
        fail = sync_data.get("failed_record", 0)
    except Exception as e:
        return {"status": "Error Sincronizar", "details": str(e)}

    # 4. RECONCILIAR
    try:
        r = requests.post(eps["reconciliar"], json={})
        recon_data = r.json()
        # Lógica básica de extracción de IDs
        ids = []
        if isinstance(recon_data, list): ids = recon_data
        elif isinstance(recon_data, dict) and "data" in recon_data: ids = recon_data["data"]
        recon_count = len(ids)
    except Exception as e:
        return {"status": "Error Reconciliar", "details": str(e)}

    # Determinar estado final
    status = "Procesado Exitosamente"
    if fail > 0: status = "Procesado con Fallos"
    if proc == 0 and fail == 0: status = "Sin Datos Nuevos"
    
    return {
        "status": status,
        "processed": proc,
        "failed": fail,
        "reconciled": recon_count,
        "details": "OK"
    }
