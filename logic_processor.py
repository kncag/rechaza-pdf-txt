import time
import requests
from io import BytesIO

# --- CONFIGURACIÓN Y REGLAS (IGUAL) ---
RECONCILE_RETRY_CONFIG = [
    (20,  3), (40,  4), (50,  5), (60,  8), (float("inf"), 10),
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

# --- HELPERS ---
def get_reconcile_retries(line_count):
    for max_lines, retries in RECONCILE_RETRY_CONFIG:
        if line_count <= max_lines: return retries
    return 4

def find_subscription_id(filename, rules_list):
    fname_lower = filename.lower()
    for sub_id, inc, exc in rules_list:
        if any(i in fname_lower for i in inc) and not any(e in fname_lower for e in exc):
            return sub_id
    return None

def validar_contenido(filename, content_str):
    lines = content_str.splitlines()
    count = len(lines)
    base = filename.lower()
    if (base.startswith("sbp") or base.startswith("bws")) and count <= 1:
        return False, "SBP/BWS Vacío", count
    if base.startswith("210309") and count == 0:
        return False, "210309 Vacío", count
    return True, "OK", count

def extraer_conteo_procesar(json_response):
    """
    Analiza la respuesta de /procesar para ver cuántos registros detectó el parseo.
    Busca en processes -> steps -> processed_record
    """
    proc_total = 0
    fail_total = 0
    try:
        # La respuesta suele tener una lista 'processes' o ser un dict directo
        processes = json_response.get("processes", [])
        if not processes and isinstance(json_response, dict):
            processes = [json_response]
            
        for p in processes:
            steps = p.get("steps", [])
            for s in steps:
                # Sumamos lo que encuentre en los pasos de 'Parse'
                proc_total += s.get("processed_record", 0)
                # OJO: A veces failed_record está en un paso específico, sumamos todo por seguridad
                fail_total += s.get("failed_record", 0)
    except:
        pass
    return proc_total, fail_total

# --- BUCLES API ---
def loop_reconciliar(session, url, target_count, line_count):
    logs = []
    max_runs = get_reconcile_retries(line_count)
    last_count, prev_count, no_change = 0, -1, 0
    
    for i in range(max_runs):
        try:
            r = session.post(url, json={})
            try: d = r.json()
            except: d = []
            ids = d if isinstance(d, list) else d.get("data", d.get("steps", []))
            last_count = len(ids)
            logs.append(f"   🔸 [RECONC #{i+1}] IDs: {last_count} (Target: {target_count})")
            
            if last_count == target_count and target_count > 0: 
                logs.append("   ✅ Target alcanzado.")
                break
            
            if last_count == prev_count:
                no_change += 1
                if no_change >= 3: 
                    logs.append("   ⚠️ Sin cambios (x3), saliendo.")
                    break
            else: no_change = 0
            prev_count = last_count
            time.sleep(1)
        except Exception as e:
            logs.append(f"   ❌ [RECONC #{i+1}] Error: {str(e)}")
            continue
    return last_count, logs

# --- FLUJO PRINCIPAL INTELIGENTE ---
def api_upload_flow(file_bytes, filename, sub_id, flow_key, line_count):
    eps = ENDPOINTS[flow_key]
    execution_logs = []
    
    file_size = len(file_bytes)
    if file_size == 0:
        return {"status": "❌ Error Bytes", "details": "0 bytes", "proc": 0, "rec": 0, "logs": ["❌ 0 bytes detectados"]}
    
    session = requests.Session()
    file_stream = BytesIO(file_bytes)
    execution_logs.append(f"📦 Enviando {file_size} bytes...")

    # VARIABLES DE ESTADO
    proc_detected = 0
    fail_detected = 0

    try:
        # 1. SUBIR
        files = {"edt": (filename, file_stream)}
        data = {"subscription_public_id": sub_id}
        session.post(eps["subir"], files=files, data=data).raise_for_status()
        execution_logs.append(f"✅ [SUBIR] OK")
        
        # 2. PROCESAR (AQUÍ LEEMOS LA RESPUESTA)
        r2 = session.post(eps["procesar"])
        r2.raise_for_status()
        
        # --- LÓGICA DE EXTRACCIÓN INMEDIATA ---
        try: 
            json_proc = r2.json()
            proc_detected, fail_detected = extraer_conteo_procesar(json_proc)
            execution_logs.append(f"✅ [PROCESAR] OK. Registros detectados: {proc_detected} | Fallidos: {fail_detected}")
        except:
            execution_logs.append("✅ [PROCESAR] OK (No se pudo leer JSON)")
        
    except Exception as e:
        execution_logs.append(f"❌ ERROR API: {str(e)}")
        return {"status": "❌ Error API", "details": str(e), "proc": 0, "rec": 0, "logs": execution_logs}

    # --- DECISIÓN INTELIGENTE ---
    if proc_detected == 0 and fail_detected == 0:
        # Si PROCESAR dice 0, no tiene sentido seguir
        execution_logs.append("🛑 [STOP] Procesar devolvió 0 registros. Terminando flujo.")
        
        # Intento opcional de reconciliar una vez por si acaso (para limpiar estados)
        try: session.post(eps["reconciliar"], json={})
        except: pass
        
        return {
            "status": "ℹ️ Sin Datos", 
            "details": "0 detectados en Parse", 
            "proc": 0, 
            "rec": 0, 
            "logs": execution_logs
        }

    # 3. SINCRONIZAR
    # Ya sabemos que HAY datos, así que llamamos a sincronizar.
    # No necesitamos bucle infinito, confiamos en lo que dijo 'procesar', 
    # pero hacemos un par de intentos por latencia.
    
    execution_logs.append(f"🔄 [SINCRONIZAR] Confirmando {proc_detected} registros...")
    sync_ok = False
    for i in range(5): # Pocos intentos, ya sabemos que hay datos
        try:
            r = session.post(eps["sincronizar"])
            d = r.json()
            if isinstance(d, list) and d: d = d[0]
            # Validar que sincronizar confirme lo que vimos en procesar
            p = d.get("processed_record", 0)
            if p > 0 or d.get("failed_record", 0) > 0:
                execution_logs.append(f"   🔹 [SINCR] Confirmado: {p}")
                sync_ok = True
                break
            time.sleep(1)
        except: pass
    
    if not sync_ok:
        execution_logs.append("⚠️ [SINCR] Alerta: API no confirmó los datos detectados.")

    # 4. RECONCILIAR
    # Usamos los fallos detectados en el paso 2 como target
    target = fail_detected
    
    recon_total, recon_logs = loop_reconciliar(session, eps["reconciliar"], target, line_count)
    execution_logs.extend(recon_logs)
    
    status = "✅ Exitoso"
    if fail_detected > 0: status = "⚠️ Con Fallos"
    if not sync_ok: status = "⚠️ Error Sincronización"

    return {
        "status": status, 
        "details": f"Detectados: {proc_detected}/{fail_detected}", 
        "proc": proc_detected, 
        "rec": recon_total, 
        "logs": execution_logs
    }
