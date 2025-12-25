from fastapi import FastAPI
from api.routes import router

app = FastAPI(title="Kasparro Backend API")

app.include_router(router)
