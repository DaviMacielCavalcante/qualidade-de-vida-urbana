from fastapi import APIRouter, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from .dependencies import get_current_user
from .jwt_handler import create_access_token
from ..database import get_db
from ..schemas.user_schema import UserRead
from ..use_cases.auth_use_case import LoginUseCase
from ..use_cases.user_use_case import User, UserCreate, CreateUserUseCase

router = APIRouter(prefix="/auth", tags=["auth"])

@router.get("/me")
def myself(
    current_user: User = Depends(get_current_user)
) -> UserRead:
   
    return current_user
   
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
def logout(response: Response):
    response.delete_cookie(key="access_token")
    return {"message": "Logout successful"}
    

@router.post("/register")
def register(
    response: Response,
    user_data: UserCreate,
    db: Session = Depends(get_db)
) -> UserRead:
    
    try:
        user = CreateUserUseCase.execute(db=db, user_create=user_data)
    except ValueError as e:
        if "already used" in str(e):
            raise HTTPException(status_code=409, detail=str(e))
        
        raise HTTPException(status_code=400, detail=str(e))

    token = create_access_token(data={"sub": str(user.id)})


    response.set_cookie(
            key="access_token",
            value=token,
            httponly=True,
            max_age=900,
            secure=False,
            samesite="lax"
        )
    
    return user