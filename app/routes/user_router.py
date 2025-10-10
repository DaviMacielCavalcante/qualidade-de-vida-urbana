from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.schemas.user_schema import UserCreate, UserRead
from app.use_cases.user_use_case import CreateUserUseCase

router = APIRouter(prefix="/users", tags=["users"])

@router.post("/", response_model=UserRead, status_code=201)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    try:
        return CreateUserUseCase.execute(db, user)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))