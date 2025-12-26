from fastapi import FastAPI
from api.routes import router
from core.db_init import init_db

app = FastAPI(title="Kasparro Backend API")

# Initialize DB (creates tables if missing)
@app.on_event("startup")
def startup_event():
    init_db()

# Register routes
app.include_router(router)
