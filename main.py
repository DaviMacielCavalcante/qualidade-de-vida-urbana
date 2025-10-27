from fastapi import FastAPI
from app.routes.user_router import router as user_router
from app.auth.auth_router import router as auth_router
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()
app.include_router(user_router)
app.include_router(auth_router)