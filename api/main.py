from fastapi import FastAPI
from api.routes import router
from core.db_init import init_tables

app = FastAPI(title="Kasparro Backend API")

# Create DB tables at startup
@app.on_event("startup")
def startup_event():
    init_tables()

app.include_router(router)
