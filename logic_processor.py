import time
import requests
from io import BytesIO

# --- CONFIGURACIÓN ---
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
    proc_total = 0
    fail_total = 0
    try:
        processes = json_response.get("processes", [])
        if not processes and isinstance(json_response, dict): processes = [json_response]
        for p in processes:
            for s in p.get("steps", []):
                proc_total += s.get("processed_record", 0)
                fail_total += s.get("failed_record", 0)
    except: pass
    return proc_total, fail_total

# --- BUCLES ROBUSTOS ---

def loop_sincronizar_robusto(session, url, expected_count):
    """
    Intenta sincronizar esperando a que el servidor confirme la cantidad esperada.
    """
    logs = []
    
    # Damos hasta 10 intentos (aprox 20 segundos)
    for i in range(10):
        try:
            r = session.post(url)
            d = r.json()
            if isinstance(d, list) and d: d = d[0]
            
            p = d.get("processed_record", 0)
            f = d.get("failed_record", 0)
            
            logs.append(f"   🔹 [SINCR #{i+1}] Proc: {p} | Fail: {f}")
            
            # Si la suma de procesados + fallidos es igual o mayor a lo que detectamos
            # en el paso anterior, significa que terminó.
            if (p + f) >= expected_count:
                return p, f, logs
                
            # Si no, esperamos un poco más
            time.sleep(2)
        except Exception as e:
            logs.append(f"   ❌ Error Sync: {e}")
            time.sleep(1)
            
    # Si se acaba el tiempo, devolvemos lo que tengamos
    logs.append("   ⚠️ Timeout en Sincronización (No se confirmaron todos los registros)")
    return 0, 0, logs

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
            
            logs.append(f"   🔸 [RECONC #{i+1}] IDs: {last_count}")
            
            if last_count == target_count and target_count > 0: 
                logs.append("   ✅ Target alcanzado.")
                break
            
            if last_count == prev_count:
                no_change += 1
                if no_change >= 2: break
            else: no_change = 0
            prev_count = last_count
            time.sleep(1)
        except Exception as e:
            logs.append(f"   ❌ Error Reconciliar: {str(e)}")
            continue
    return last_count, logs

# --- FLUJO PRINCIPAL ---
def api_upload_flow(file_bytes, filename, sub_id, flow_key, line_count):
    eps = ENDPOINTS[flow_key]
    execution_logs = []
    
    if len(file_bytes) == 0:
        return {"status": "❌ Error Bytes", "details": "0 bytes", "proc": 0, "rec": 0, "logs": ["❌ 0 bytes"]}
    
    session = requests.Session()
    file_stream = BytesIO(file_bytes)
    execution_logs.append(f"📦 Enviando {len(file_bytes)} bytes...")

    # VARIABLES
    proc_detected = 0
    fail_detected = 0

    try:
        # 1. SUBIR
        files = {"edt": (filename, file_stream)}
        data = {"subscription_public_id": sub_id}
        session.post(eps["subir"], files=files, data=data).raise_for_status()
        execution_logs.append("✅ [SUBIR] OK")
        
        # 2. PROCESAR
        r2 = session.post(eps["procesar"])
        r2.raise_for_status()
        
        try: 
            json_proc = r2.json()
            proc_detected, fail_detected = extraer_conteo_procesar(json_proc)
            execution_logs.append(f"✅ [PROCESAR] OK. Detectados: {proc_detected} | Fallidos: {fail_detected}")
        except:
            execution_logs.append("✅ [PROCESAR] OK (JSON ilegible)")
        
    except Exception as e:
        execution_logs.append(f"❌ ERROR API: {str(e)}")
        return {"status": "❌ Error API", "details": str(e), "proc": 0, "rec": 0, "logs": execution_logs}

    # --- DECISIÓN ---
    if proc_detected == 0 and fail_detected == 0:
        execution_logs.append("🛑 Sin datos detectados en Parse. Terminando.")
        try: session.post(eps["reconciliar"], json={}) 
        except: pass
        return {"status": "ℹ️ Sin Datos", "details": "0 registros", "proc": 0, "rec": 0, "logs": execution_logs}

    # 3. SINCRONIZAR (USANDO EL BUCLE ROBUSTO NUEVO)
    execution_logs.append(f"🔄 [SINCRONIZAR] Esperando confirmación de {proc_detected} registros...")
    
    # Esperamos que la suma de (proc + fail) llegue al menos a lo que detectamos
    target_sync = proc_detected + fail_detected
    
    final_proc, final_fail, sync_logs = loop_sincronizar_robusto(session, eps["sincronizar"], target_sync)
    execution_logs.extend(sync_logs)

    # 4. RECONCILIAR
    # Usamos los fallos detectados como target
    target_rec = final_fail
    
    # Logica de estado final
    status = "✅ Exitoso"
    if final_fail > 0: status = "⚠️ Con Fallos"
    if final_proc == 0 and final_fail == 0: status = "⚠️ Error Sincronización"

    recon_total, recon_logs = loop_reconciliar(session, eps["reconciliar"], target_rec, line_count)
    execution_logs.extend(recon_logs)

    return {
        "status": status, 
        "details": f"Reg: {final_proc}/{final_fail}", 
        "proc": final_proc, 
        "rec": recon_total, 
        "logs": execution_logs
    }
