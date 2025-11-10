from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from app.database import get_db
from app.schemas.user_schema import UserCreate, UserRead, UserUpdate
from app.use_cases.user_use_case import CreateUserUseCase, GetUserByEmailUseCase, GetAllUsersUseCase, UpdateUserUseCase,DeleteUserByEmailUseCase
from ..auth.dependencies import get_current_user, require_admin, validate_user_or_admin
from ..models.user_model import User

router = APIRouter(prefix="/users", tags=["users"])

@router.get("/email", response_model=UserRead, status_code=200, responses={
    200: {"description": "User found"},
    401: {"description": "Not authenticated!"},
    404: {
        "description": "User not found",
        "content": {
            "application/json": {
                "example": {"detail": "User with email john@example.com not found"}
            }
        }
    },
    422: {"description": "Invalid email"}
})
def get_user_by_email(
    email: str = Query(..., description="The email of the user to retrieve"), 
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin)
    ):
    try:
        return GetUserByEmailUseCase.execute(db, email)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    

@router.get("/all", response_model=list[UserRead], status_code=200, responses={
    200: {"description": "List of users retrieved successfully"},
    400 : {"description": "Invalid parameters"},
    401: {"description": "Not authenticated!"}
})
def get_all_users(db: Session = Depends(get_db), skip: int = 0, limit: int = 100, current_user: User = Depends(require_admin)):

    
    try:
        return GetAllUsersUseCase.execute(db, skip, limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/", response_model=UserRead, status_code=201, responses={
    201: {"description": "User created successfully"},
    409: {
        "description": "Email already exists",
        "content": {
            "application/json": {
                "example": {"detail": "User with this email already exists"}
            }
        }
    },
    422: {"description": "Invalid data"}
})
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    try:
        return CreateUserUseCase.execute(db, user)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    

@router.put("/update", response_model= UserRead, status_code=200, responses={
    200: {"description": "User updated"},
    403: {"description": "Operation denied"},
    404: {
        "description": "User not found",
        "content": {
            "application/json": {
                "example": {"detail": "User with email john@example.com not found"}
            }
        }
    },
    409: {
        "description": "Email already exists in the database",
        "content": {
                "application/json": {
                    "example": {"detail": "Email jane@example.com already in use by another user"}
                }
        }
    },
    422: {"description": "Invalid data or no fields to update"}
})
def update_user(
    email: str=Query(..., description="The email of the user to update"),
    user: UserUpdate = ...,
    db: Session=Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    
    validate_user_or_admin(email=email, current_user=current_user)

    try:
        return UpdateUserUseCase.execute(db, email, user)
    except ValueError as e:
        if "not found" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        if "already used" in str(e):
            raise HTTPException(status_code=409, detail=str(e))
        if "no fields" in str(e):
            raise HTTPException(status_code=422, detail=str(e))

    
@router.delete("/delete", response_model=UserRead, status_code=200, responses={
    200: {"description": "User deleted"},
    403: {"description": "Operation denied"},
    404: {
        "description": "User not found",
        "content": {
            "application/json": {
                "example": {"detail": "User with email john@example.com not found"}
            }
        }
    },
    422: {"description": "Invalid email"}
})
def deleter_user(
    email: str=Query(..., description="The email of the user to retrieve"), 
    db: Session=Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    
    try:
        target_user = GetUserByEmailUseCase.execute(db, email)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    
    if current_user.role == "admin" and current_user.email == email:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operation denied"
        )
    
    if current_user.role == "admin" and target_user.role == "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operation denied"
        )
    
    if current_user.role != "admin" and current_user.email != email:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operation denied"
        )
    
    return DeleteUserByEmailUseCase.execute(db, email)
    