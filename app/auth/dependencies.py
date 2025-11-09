from sqlalchemy.orm import Session
from fastapi import status, Depends, Cookie
from fastapi.exceptions import HTTPException
from jose import JWTError
from .jwt_handler import verify_token
from ..repositories.user_repository import user_repository
from ..models.user_model import User
from ..database import get_db

def get_current_user(
        access_token: str = Cookie(None),
        db: Session = Depends(get_db)
) -> User:

    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )

    try:
        payload = verify_token(access_token)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )
    
    user_id = payload.get("sub")

    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )
    
    user = user_repository.get_user_by_id(db=db, user_id=user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )
    
    return user