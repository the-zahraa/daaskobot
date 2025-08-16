# backend/app/main.py
import os
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

ADMIN_SECRET = os.getenv("ADMIN_SECRET", "changeme")
FRONTEND_WEBAPP_URL = os.getenv("FRONTEND_WEBAPP_URL", "")

app = FastAPI(title="Telegram Analytics & Management API")

# CORS for local dev of Vite miniapp
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health", response_class=JSONResponse)
def health():
    return {"status": "ok"}

# --- Owner admin (global) ---
def require_admin(req: Request):
    if req.headers.get("x-admin-secret") != ADMIN_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/admin", response_class=HTMLResponse)
def owner_panel(req: Request):
    # simple page; protect with header in production (or behind login)
    return HTMLResponse(f"""
    <html><head><title>Owner Admin</title></head>
    <body style="font-family: Inter, Arial; padding:16px;">
      <h2>Owner Admin Panel</h2>
      <p>Use API endpoints (secured by <code>X-Admin-Secret</code>) to manage tenants/users.</p>
      <ul>
        <li>GET <code>/admin/tenants</code></li>
        <li>POST <code>/admin/tenants</code> (JSON: name, owner_user_id)</li>
        <li>GET <code>/admin/usage</code></li>
      </ul>
      <p>For now this is a placeholder UI. Your bot’s <b>/admin</b> command opens your owner view in the mini-app too.</p>
      <a href="{FRONTEND_WEBAPP_URL}?view=owner">Open Owner Mini-App</a>
    </body></html>
    """)

@app.get("/admin/tenants", response_class=JSONResponse)
def list_tenants(req: Request):
    require_admin(req)
    # TODO: fetch from DB; placeholder
    return [{"id":"demo-tenant-id","name":"Demo Tenant","owner_user_id":123456789}]

@app.post("/admin/tenants", response_class=JSONResponse)
async def create_tenant(req: Request):
    require_admin(req)
    data = await req.json()
    # TODO: insert into DB; placeholder echo
    return {"ok": True, "tenant": data}

@app.get("/admin/usage", response_class=JSONResponse)
def usage(req: Request):
    require_admin(req)
    # TODO: pull usage stats; placeholder
    return {"active_users": 0, "active_chats": 0}

# --- Tenant UI (customer) ---
@app.get("/tenant", response_class=HTMLResponse)
def tenant_panel():
    return HTMLResponse(f"""
    <html><head><title>Tenant Panel</title></head>
    <body style="font-family: Inter, Arial; padding:16px;">
      <h2>Tenant Panel</h2>
      <p>Manage your group/channel analytics, invite links and reports.</p>
      <ul>
        <li>Joins by day/week/month</li>
        <li>Invite link analytics</li>
        <li>Export reports</li>
      </ul>
      <a href="{FRONTEND_WEBAPP_URL}?view=tenant">Open Tenant Mini-App</a>
    </body></html>
    """)
