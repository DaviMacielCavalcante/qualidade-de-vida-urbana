from fastapi import APIRouter, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from ..database import get_db
from ..use_cases.auth_use_case import LoginUseCase

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/login")
def login(
    response: Response,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):

    try:
        result = LoginUseCase.execute(db, form_data.username, form_data.password)

        response.set_cookie(
            key="access_token",
            value=result["access_token"],
            httponly=True,
            max_age=900,
            secure=False,
            samesite="lax"
        )

        return {"message": "Login successful"}
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"}
            )
    

@router.post("/logout")
def logou(response: Response):
    response.delete_cookie(key="access_token")
    return {"message": "Logout successful"}
    
    

    
