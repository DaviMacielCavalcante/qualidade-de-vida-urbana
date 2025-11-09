from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.user_router import router as user_router
from app.auth.auth_router import router as auth_router
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()
app.include_router(user_router)
app.include_router(auth_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "null"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)