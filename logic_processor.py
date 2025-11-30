import time
import requests

# --- CONFIGURACIÓN DE REINTENTOS ---
RECONCILE_RETRY_CONFIG = [
    (20,  3), (40,  4), (50,  5), (60,  8), (float("inf"), 10),
]

# --- ENDPOINTS ---
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

# --- REGLAS ---
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

# --- HELPERS (AQUÍ ESTÁ LA FUNCIÓN QUE FALTABA) ---

def get_reconcile_retries(line_count):
    for max_lines, retries in RECONCILE_RETRY_CONFIG:
        if line_count <= max_lines: return retries
    return 4

def find_subscription_id(filename, rules_list):
    """Busca ID de suscripción en una lista específica de reglas."""
    fname_lower = filename.lower()
    for sub_id, inc, exc in rules_list:
        if any(i in fname_lower for i in inc) and not any(e in fname_lower for e in exc):
            return sub_id
    return None

def validar_contenido(filename, content_str):
    """Valida si el archivo es apto para procesar."""
    lines = content_str.splitlines()
    count = len(lines)
    base = filename.lower()
    
    if (base.startswith("sbp") or base.startswith("bws")) and count <= 1:
        return False, "SBP/BWS Vacío", count
    if base.startswith("210309") and count == 0:
        return False, "210309 Vacío", count
    return True, "OK", count

# --- BUCLES API ---
def loop_sincronizar(url, max_attempts=15):
    """Retorna: (init_proc, init_fail, proc, fail, logs_list)"""
    logs = []
    init_proc, init_fail = 0, 0
    
    try:
        r = requests.post(url); r.raise_for_status()
        d = r.json()
        if isinstance(d, list) and d: d = d[0]
        init_proc, init_fail = d.get("processed_record", 0), d.get("failed_record", 0)
        logs.append(f"   🔹 [SINCR #1] Proc: {init_proc} | Fail: {init_fail}")
    except Exception as e:
        logs.append(f"   ❌ [SINCR #1] Error: {str(e)}")
        return 0, 0, -1, -1, logs

    if init_proc == 0 and init_fail == 0:
        return 0, 0, 0, 0, logs
        
    proc, fail = init_proc, init_fail
    for i in range(max_attempts):
        if proc == 0 and fail == 0: break
        time.sleep(1)
        try:
            r = requests.post(url); d = r.json()
            if isinstance(d, list) and d: d = d[0]
            proc, fail = d.get("processed_record", 0), d.get("failed_record", 0)
            logs.append(f"   🔹 [SINCR #{i+2}] Proc: {proc} | Fail: {fail}")
        except:
            continue
            
    return init_proc, init_fail, proc, fail, logs

def loop_reconciliar(url, target_count, line_count):
    """Retorna: (last_count, logs_list)"""
    logs = []
    max_runs = get_reconcile_retries(line_count)
    last_count, prev_count, no_change = 0, -1, 0
    
    for i in range(max_runs):
        try:
            r = requests.post(url, json={})
            try: d = r.json()
            except: d = []
            ids = d if isinstance(d, list) else d.get("data", d.get("steps", []))
            last_count = len(ids)
            
            logs.append(f"   🔸 [RECONC #{i+1}/{max_runs}] IDs encontrados: {last_count} (Target: {target_count})")
            
            if last_count == target_count and target_count > 0: 
                logs.append("   ✅ Target alcanzado.")
                break
            
            if last_count == prev_count:
                no_change += 1
                if no_change >= 3: 
                    logs.append("   ⚠️ Sin cambios detectados, saliendo.")
                    break
            else: no_change = 0
            prev_count = last_count
            time.sleep(1)
        except Exception as e:
            logs.append(f"   ❌ [RECONC #{i+1}] Error: {str(e)}")
            continue
            
    return last_count, logs

# --- FLUJO PRINCIPAL ---
def api_upload_flow(file_bytes, filename, sub_id, flow_key, line_count):
    eps = ENDPOINTS[flow_key]
    execution_logs = [] # Aquí guardamos todo el historial
    
    try:
        # 1. Subir
        requests.post(eps["subir"], files={"edt": (filename, file_bytes)}, data={"subscription_public_id": sub_id}).raise_for_status()
        execution_logs.append("✅ [SUBIR] OK")
        
        # 2. Procesar
        requests.post(eps["procesar"]).raise_for_status()
        execution_logs.append("✅ [PROCESAR] OK")
        
    except Exception as e:
        execution_logs.append(f"❌ ERROR API: {str(e)}")
        return {"status": "❌ Error API", "details": str(e), "proc": 0, "rec": 0, "logs": execution_logs}

    # 3. Sincronizar
    ip, ifail, _, _, sync_logs = loop_sincronizar(eps["sincronizar"])
    execution_logs.extend(sync_logs)
    
    # 4. Reconciliar
    recon_total, status = 0, ""
    recon_logs = []
    
    if ip == 0 and ifail == 0:
        requests.post(eps["reconciliar"], json={})
        status = "ℹ️ Sin Datos"
        execution_logs.append("ℹ️ [RECONC] Omitido (Sin datos en sincr)")
    elif ifail == ip:
        requests.post(eps["reconciliar"], json={})
        status = "⚠️ Ya Procesado"
        execution_logs.append("⚠️ [RECONC] Omitido (Ya procesado anteriormente)")
    else:
        target = ifail if (ip > 0 and ifail > 0) else 0
        recon_total, recon_logs = loop_reconciliar(eps["reconciliar"], target, line_count)
        execution_logs.extend(recon_logs)
        
        status = "⚠️ Con Fallos" if (ifail > 0 and ifail != ip) else "✅ Exitoso"

    return {
        "status": status, 
        "details": f"Sync: {ip}/{ifail}", 
        "proc": ip, 
        "rec": recon_total,
        "logs": execution_logs # <--- Devolvemos la lista completa
    }
