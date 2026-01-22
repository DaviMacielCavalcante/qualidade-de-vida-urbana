from jose import jwt, JWTError
from datetime import datetime as dt, timedelta
from app.config import get_settings
from zoneinfo import ZoneInfo

def create_access_token(data: dict) -> str:
    
    user_data = data.copy()

    settings = get_settings()

    user_data["exp"] = dt.now(tz=ZoneInfo("UTC")) + timedelta(minutes=settings.access_token_expire_minutes)

    return jwt.encode(user_data, settings.secret_key, algorithm=settings.algorithm)

def verify_token(token: str) -> dict:

    settings = get_settings()

    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        return payload
    except JWTError as e:
        raise e