"""
PRAXA CONTROL — Cloud Backend
Fase 1: FastAPI + WebSocket Hub + JWT Auth + Persistencia JSON
Deploy: Render.com
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import json, asyncio, os, jwt
from datetime import datetime, timedelta
from typing import Optional
from pydantic import BaseModel
from pathlib import Path

# ─────────────────────────────────────────────
#  CONFIG (variables de entorno — nunca hardcoded en producción)
# ─────────────────────────────────────────────
SECRET_KEY  = os.getenv("SECRET_KEY",  "praxa_secret_2025_cambia_esto_en_render")
SCADA_KEY   = os.getenv("SCADA_KEY",   "praxa_machine_key_cambia_esto_en_render")
EMPRESA_ID  = os.getenv("EMPRESA_ID",  "empresa_demo")   # identificador del cliente
DATA_DIR    = Path(os.getenv("DATA_DIR", "/tmp/praxa_data"))

DATA_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE     = DATA_DIR / "log_eventos.json"
INV_FILE     = DATA_DIR / "inventario.json"
METRICS_FILE = DATA_DIR / "metricas.json"

# ─────────────────────────────────────────────
#  APP
# ─────────────────────────────────────────────
app = FastAPI(
    title=f"PRAXA CONTROL — {EMPRESA_ID}",
    version="1.0.0",
    docs_url="/docs" if os.getenv("ENV") != "production" else None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # en producción: lista tus dominios específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer(auto_error=False)

# ─────────────────────────────────────────────
#  PERSISTENCIA SIMPLE (JSON en disco)
#  En Paquete Media esto se reemplaza por PostgreSQL
# ─────────────────────────────────────────────
def _leer_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def _escribir_json(path: Path, data):
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[PRAXA] Error escribiendo {path}: {e}")

# Estado en memoria (se rehidrata del disco al arrancar)
scada_state = {
    "estado": "OPERACION",
    "tec1": "",
    "tec2": "",
    "turno_activo": None,
    "oee": 0.0,
    "mtbf": 0.0,
    "mttr": 0.0,
    "fallas": 0,
    "ultima_actualizacion": None,
    "conectado": False,
    "empresa_id": EMPRESA_ID,
}

log_buffer:    list = _leer_json(LOG_FILE,     [])[-200:]
inventario:    dict = _leer_json(INV_FILE,     {})
metricas_hist: list = _leer_json(METRICS_FILE, [])[-100:]

# ─────────────────────────────────────────────
#  USUARIOS (en Paquete Media: tabla en DB)
# ─────────────────────────────────────────────
USUARIOS_FILE = DATA_DIR / "usuarios.json"
_default_usuarios = {
    "1008": {"nombre": "Ing. Sebastian", "rol": "ADMIN"},
    "0102": {"nombre": "Ing. Pedro",     "rol": "SUPERVISOR"},
    "6365": {"nombre": "Ing. Brandon",   "rol": "SUPERVISOR"},
    "4289": {"nombre": "Ing. Carlos",    "rol": "SUPERVISOR"},
    "1346": {"nombre": "Miguel",         "rol": "TECNICO"},
    "1010": {"nombre": "Ana",            "rol": "TECNICO"},
    "1011": {"nombre": "Sofia",          "rol": "TECNICO"},
    "1111": {"nombre": "Laura",          "rol": "TECNICO"},
    "8888": {"nombre": "Diego",          "rol": "TECNICO"},
    "5555": {"nombre": "Ricardo",        "rol": "TECNICO"},
}
USUARIOS: dict = _leer_json(USUARIOS_FILE, _default_usuarios)

# ─────────────────────────────────────────────
#  JWT
# ─────────────────────────────────────────────
def create_token(uid: str, nombre: str, rol: str) -> str:
    payload = {
        "uid": uid,
        "nombre": nombre,
        "rol": rol,
        "empresa": EMPRESA_ID,
        "exp": datetime.utcnow() + timedelta(hours=12),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    if not credentials:
        raise HTTPException(status_code=401, detail="Token requerido")
    try:
        return jwt.decode(
            credentials.credentials, SECRET_KEY, algorithms=["HS256"]
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido")

# ─────────────────────────────────────────────
#  CONNECTION MANAGER
# ─────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.clients: list[WebSocket] = []
        self.scada:   Optional[WebSocket] = None

    async def connect_client(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)
        # Enviar estado actual al nuevo cliente
        await ws.send_json({
            "type": "init",
            "state": scada_state,
            "log": log_buffer[-50:],
            "inventario": inventario,
            "empresa_id": EMPRESA_ID,
        })
        print(f"[PRAXA] Cliente conectado. Total: {len(self.clients)}")

    async def connect_scada(self, ws: WebSocket):
        await ws.accept()
        self.scada = ws
        scada_state["conectado"] = True
        await self.broadcast_clients({"type": "scada_connected"})
        print("[PRAXA] ✅ SCADA Python conectado")

    def disconnect_client(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)
        print(f"[PRAXA] Cliente desconectado. Total: {len(self.clients)}")

    def disconnect_scada(self):
        self.scada = None
        scada_state["conectado"] = False
        print("[PRAXA] ⚠ SCADA Python desconectado")

    async def broadcast_clients(self, data: dict):
        dead = []
        for client in self.clients:
            try:
                await client.send_json(data)
            except Exception:
                dead.append(client)
        for d in dead:
            self.clients.remove(d)

    async def send_to_scada(self, data: dict) -> bool:
        if not self.scada:
            return False
        try:
            await self.scada.send_json(data)
            return True
        except Exception:
            self.disconnect_scada()
            return False

manager = ConnectionManager()

# ─────────────────────────────────────────────
#  MODELOS
# ─────────────────────────────────────────────
class LoginRequest(BaseModel):
    uid: str
    password: str

class ComandoRequest(BaseModel):
    action: str
    params: dict = {}

# ─────────────────────────────────────────────
#  ENDPOINTS REST
# ─────────────────────────────────────────────

@app.get("/health")
async def health():
    """Endpoint de salud — sin auth, para Render health checks"""
    return {
        "ok": True,
        "empresa": EMPRESA_ID,
        "scada_conectado": scada_state["conectado"],
        "clientes_web": len(manager.clients),
        "ts": datetime.now().isoformat(),
    }

@app.post("/api/login")
async def login(req: LoginRequest):
    user = USUARIOS.get(req.uid)
    if not user or req.password != req.uid:
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")
    token = create_token(req.uid, user["nombre"], user["rol"])
    return {
        "token": token,
        "nombre": user["nombre"],
        "rol": user["rol"],
        "uid": req.uid,
        "empresa": EMPRESA_ID,
    }

@app.get("/api/estado")
async def get_estado(payload: dict = Depends(verify_token)):
    return scada_state

@app.get("/api/log")
async def get_log(
    limit: int = Query(100, le=500),
    payload: dict = Depends(verify_token),
):
    return {"log": log_buffer[-limit:], "total": len(log_buffer)}

@app.get("/api/inventario")
async def get_inventario(payload: dict = Depends(verify_token)):
    return inventario

@app.get("/api/metricas")
async def get_metricas(
    limit: int = Query(100, le=500),
    payload: dict = Depends(verify_token),
):
    return {"historia": metricas_hist[-limit:], "total": len(metricas_hist)}

@app.get("/api/usuarios")
async def get_usuarios(payload: dict = Depends(verify_token)):
    if payload.get("rol") not in ("ADMIN", "SUPERVISOR"):
        raise HTTPException(status_code=403, detail="Sin permiso")
    return {uid: {"nombre": u["nombre"], "rol": u["rol"]} for uid, u in USUARIOS.items()}

@app.post("/api/emergencia")
async def emergencia(payload: dict = Depends(verify_token)):
    ok = await manager.send_to_scada({
        "type": "command",
        "action": "emergencia",
        "usuario": payload.get("nombre"),
    })
    _log_evento(f"[MOBILE] EMERGENCIA activada por {payload.get('nombre')}")
    return {"ok": True, "scada_recibio": ok}

@app.post("/api/reanudar")
async def reanudar(payload: dict = Depends(verify_token)):
    ok = await manager.send_to_scada({
        "type": "command",
        "action": "reanudar",
        "usuario": payload.get("nombre"),
    })
    _log_evento(f"[MOBILE] Reanudación solicitada por {payload.get('nombre')}")
    return {"ok": True, "scada_recibio": ok}

@app.post("/api/comando")
async def comando(req: ComandoRequest, payload: dict = Depends(verify_token)):
    """Endpoint genérico para comandos futuros"""
    if payload.get("rol") not in ("ADMIN", "SUPERVISOR"):
        raise HTTPException(status_code=403, detail="Sin permiso")
    ok = await manager.send_to_scada({
        "type": "command",
        "action": req.action,
        "params": req.params,
        "usuario": payload.get("nombre"),
    })
    return {"ok": True, "scada_recibio": ok}

# ─────────────────────────────────────────────
#  WEBSOCKET — SCADA PYTHON
# ─────────────────────────────────────────────
@app.websocket("/ws/scada")
async def ws_scada(ws: WebSocket, key: str = ""):
    if key != SCADA_KEY:
        await ws.close(code=4001)
        print("[PRAXA] ⛔ Intento de conexión SCADA con clave inválida")
        return

    await manager.connect_scada(ws)
    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            await _handle_scada_message(msg)
    except WebSocketDisconnect:
        manager.disconnect_scada()
        await manager.broadcast_clients({"type": "scada_disconnected"})

async def _handle_scada_message(msg: dict):
    global scada_state, log_buffer, inventario, metricas_hist
    t = msg.get("type")

    if t == "estado":
        data = msg.get("data", {})
        scada_state.update(data)
        scada_state["ultima_actualizacion"] = datetime.now().isoformat()
        scada_state["conectado"] = True
        await manager.broadcast_clients({"type": "estado", "data": scada_state})

    elif t == "log":
        lineas = msg.get("lineas", [])
        log_buffer.extend(lineas)
        log_buffer[:] = log_buffer[-200:]
        # Persistir cada 10 eventos para no saturar disco
        if len(log_buffer) % 10 == 0:
            _escribir_json(LOG_FILE, log_buffer)
        await manager.broadcast_clients({"type": "log", "lineas": lineas})

    elif t == "inventario":
        inventario = msg.get("data", {})
        _escribir_json(INV_FILE, inventario)
        await manager.broadcast_clients({"type": "inventario", "data": inventario})

    elif t == "metricas":
        kpi = msg.get("data", {})
        kpi["ts"] = datetime.now().isoformat()
        metricas_hist.append(kpi)
        metricas_hist[:] = metricas_hist[-100:]
        # Persistir histórico de métricas
        if len(metricas_hist) % 5 == 0:
            _escribir_json(METRICS_FILE, metricas_hist)
        await manager.broadcast_clients({"type": "metricas", "data": kpi})

# ─────────────────────────────────────────────
#  WEBSOCKET — CLIENTES WEB / MÓVIL
# ─────────────────────────────────────────────
@app.websocket("/ws/client")
async def ws_client(ws: WebSocket, token: str = ""):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except Exception:
        await ws.close(code=4001)
        return

    await manager.connect_client(ws)
    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            # El cliente puede enviar comandos al SCADA
            if msg.get("type") == "command":
                msg["usuario"] = payload.get("nombre")
                await manager.send_to_scada(msg)
    except WebSocketDisconnect:
        manager.disconnect_client(ws)

# ─────────────────────────────────────────────
#  HELPERS INTERNOS
# ─────────────────────────────────────────────
def _log_evento(texto: str):
    ahora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    linea = f"[{ahora}][CLOUD] {texto}"
    log_buffer.append(linea)
    log_buffer[:] = log_buffer[-200:]

# ─────────────────────────────────────────────
#  ARRANQUE LOCAL (desarrollo)
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
