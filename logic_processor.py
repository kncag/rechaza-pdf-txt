# logic_processor.py
import requests

# --- Configuración de Endpoints ---
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

# --- Reglas de Clasificación ---
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

def detect_system_and_subscription(filename):
    """
    Intenta determinar si el archivo pertenece a EURO o UDEP
    y cuál es su ID de suscripción basándose en el nombre.
    Retorna: (nombre_sistema, id_suscripcion, flow_key)
    """
    fname_lower = filename.lower()
    
    # 1. Intentar con reglas EURO
    for sub_id, include, exclude in RULES_EURO:
        # Lógica de coincidencia: debe tener algun 'include' y NINGUN 'exclude'
        # Usamos nombre base simple, asumiendo que el usuario sube el archivo directo
        if any(inc in fname_lower for inc in include) and not any(exc in fname_lower for exc in exclude):
            return "EURO", sub_id, "euro"
            
    # 2. Intentar con reglas UDEP
    for sub_id, include, exclude in RULES_UDEP:
        if any(inc in fname_lower for inc in include) and not any(exc in fname_lower for exc in exclude):
            return "UDEP", sub_id, "udep"
            
    return None, None, None

def api_upload_flow(file_bytes, filename, sub_id, flow_type):
    """Ejecuta la secuencia de carga en la API."""
    eps = ENDPOINTS[flow_type]
    results = {}
    
    # 1. SUBIR
    try:
        files = {"edt": (filename, file_bytes)}
        data = {"subscription_public_id": sub_id}
        r = requests.post(eps["subir"], files=files, data=data)
        r.raise_for_status()
    except Exception as e:
        return {"status": "Error Subida", "details": str(e), "processed": 0, "reconciled": 0}

    # 2. PROCESAR
    try:
        requests.post(eps["procesar"]).raise_for_status()
    except Exception as e:
        return {"status": "Error Procesar", "details": str(e), "processed": 0, "reconciled": 0}
        
    # 3. SINCRONIZAR
    try:
        r = requests.post(eps["sincronizar"])
        sync_data = r.json()
        if isinstance(sync_data, list) and sync_data: sync_data = sync_data[0]
        elif not isinstance(sync_data, dict): sync_data = {}
        
        proc = sync_data.get("processed_record", 0)
        fail = sync_data.get("failed_record", 0)
    except Exception as e:
        return {"status": "Error Sincronizar", "details": str(e), "processed": 0, "reconciled": 0}

    # 4. RECONCILIAR
    try:
        r = requests.post(eps["reconciliar"], json={})
        # Manejo robusto de respuesta
        try:
            recon_data = r.json()
        except:
            recon_data = []
            
        ids = []
        if isinstance(recon_data, list): ids = recon_data
        elif isinstance(recon_data, dict):
            if "data" in recon_data: ids = recon_data["data"]
            elif "steps" in recon_data: ids = recon_data["steps"]
        
        recon_count = len(ids)
    except Exception as e:
        return {"status": "Error Reconciliar", "details": str(e), "processed": proc, "reconciled": 0}

    # Estado final
    if fail > 0: 
        status = "⚠️ Procesado con Fallos"
    elif proc == 0 and fail == 0: 
        status = "ℹ️ Sin Datos Nuevos"
    else: 
        status = "✅ Procesado Exitosamente"
    
    return {
        "status": status,
        "processed": proc,
        "reconciled": recon_count,
        "details": "Flujo completado OK"
    }
