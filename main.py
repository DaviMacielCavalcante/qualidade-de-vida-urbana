from app.routes.user_router import router 
from fastapi import FastAPI

app = FastAPI()
app.include_router(router)