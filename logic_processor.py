import time
import requests

# --- CONFIGURACIÓN DE REINTENTOS (Original Local) ---
# (Num Líneas, Max Reintentos)
RECONCILE_RETRY_CONFIG = [
    (20,  3),
    (40,  4),
    (50,  5),
    (60,  8),
    (float("inf"), 10),
]

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

# Reglas de Clasificación
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

# --- UTILIDADES ---

def get_reconcile_retries(line_count):
    """Calcula intentos según el tamaño del archivo."""
    for max_lines, retries in RECONCILE_RETRY_CONFIG:
        if line_count <= max_lines:
            return retries
    return 4

def detect_system_and_subscription(filename):
    fname_lower = filename.lower()
    for sub_id, inc, exc in RULES_EURO:
        if any(i in fname_lower for i in inc) and not any(e in fname_lower for e in exc):
            return "EURO", sub_id, "euro"
    for sub_id, inc, exc in RULES_UDEP:
        if any(i in fname_lower for i in inc) and not any(e in fname_lower for e in exc):
            return "UDEP", sub_id, "udep"
    return None, None, None

def validar_contenido(filename, content_str):
    """Retorna (es_valido, razon, num_lineas)"""
    lines = content_str.splitlines()
    count = len(lines)
    base = filename.lower()
    
    if (base.startswith("sbp") or base.startswith("bws")) and count <= 1:
        return False, "sbp/bws vacio", count
    if base.startswith("210309") and count == 0:
        return False, "210309 vacio", count
    # Permitimos vacíos genéricos pero los marcamos para alerta
    return True, "OK", count

# --- LÓGICA DE API CON REINTENTOS ---

def post_sincronizar_safe(url, max_http_retries=3):
    """Llama a sincronizar manejando errores 504 internamente."""
    for _ in range(max_http_retries):
        try:
            r = requests.post(url)
            r.raise_for_status()
            d = r.json()
            if isinstance(d, list) and d: d = d[0]
            return d.get("processed_record", 0), d.get("failed_record", 0)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 504:
                time.sleep(1) # Esperar antes de reintentar HTTP
                continue
            raise e
        except:
            time.sleep(1)
            continue
    raise Exception("Fallo sincronizar tras reintentos HTTP")

def loop_sincronizar(url, max_attempts=15):
    """Repite la sincronización hasta obtener datos o agotar intentos."""
    try:
        init_proc, init_fail = post_sincronizar_safe(url)
    except:
        return 0, 0, -1, -1 # Fallo inicial

    if init_proc == 0 and init_fail == 0:
        return 0, 0, 0, 0 # Nada que procesar
        
    proc, fail = init_proc, init_fail
    for _ in range(max_attempts):
        if proc == 0 and fail == 0: break # Terminó
        try:
            proc, fail = post_sincronizar_safe(url)
        except:
            continue
            
    return init_proc, init_fail, proc, fail

def loop_reconciliar(url, target_count, line_count):
    """Reconcilia usando la tabla de configuración dinámica."""
    max_runs = get_reconcile_retries(line_count)
    
    last_count = 0
    prev_count = -1
    no_change = 0
    
    for _ in range(max_runs):
        try:
            r = requests.post(url, json={})
            try: d = r.json()
            except: d = []
            
            ids = []
            if isinstance(d, list): ids = d
            elif isinstance(d, dict):
                ids = d.get("data", d.get("steps", []))
            
            last_count = len(ids)
            
            if last_count == target_count and target_count > 0: 
                break # Éxito exacto
            
            # Lógica de estabilidad: si el número no cambia 3 veces, salimos
            if last_count == prev_count:
                no_change += 1
                if no_change >= 3: break
            else:
                no_change = 0
            prev_count = last_count
            time.sleep(1)
        except:
            continue
            
    return last_count

def api_upload_flow(file_bytes, filename, sub_id, flow_type, line_count):
    eps = ENDPOINTS[flow_type]
    
    # 1. SUBIR
    try:
        files = {"edt": (filename, file_bytes)}
        data = {"subscription_public_id": sub_id}
        requests.post(eps["subir"], files=files, data=data).raise_for_status()
    except Exception as e:
        return {"status": "❌ Error Subida", "details": str(e), "proc": 0, "rec": 0}

    # 2. PROCESAR
    try:
        requests.post(eps["procesar"]).raise_for_status()
    except Exception as e:
        return {"status": "❌ Error Procesar", "details": str(e), "proc": 0, "rec": 0}

    # 3. SINCRONIZAR (Con Loop original)
    ip, ifail, lp, lfail = loop_sincronizar(eps["sincronizar"])
    
    # 4. RECONCILIAR (Lógica original)
    recon_total = 0
    status = ""
    
    if ip == 0 and ifail == 0:
        # Caso 1: Sin datos
        try:
            requests.post(eps["reconciliar"], json={})
        except: pass
        status = "ℹ️ Sin Datos (Vacío)"
    elif ifail == ip:
        # Caso 2: Ya procesado anteriormente
        try:
            requests.post(eps["reconciliar"], json={})
        except: pass
        status = "⚠️ Ya Procesado Anteriormente"
    else:
        # Caso 3: Nuevo procesamiento
        target = ifail if (ip > 0 and ifail > 0) else 0
        recon_total = loop_reconciliar(eps["reconciliar"], target, line_count)
        
        if ifail > 0 and ifail != ip: 
            status = "⚠️ Procesado con Fallos"
        else: 
            status = "✅ Procesado Exitosamente"

    return {
        "status": status,
        "details": f"Sync Init: {ip}/{ifail}",
        "proc": ip,
        "rec": recon_total
    }
