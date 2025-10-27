from sqlalchemy.orm import Session
from ..repositories.user_repository import UserRepository, user_repository, check_hashed_pwd
from ..auth.jwt_handler import create_access_token

class LoginUseCase:

    def __init__(self, repository: UserRepository = user_repository):
        self.repository = repository

    def execute(self, db: Session, email: str, password: str) -> dict:
        user = self.repository.get_user_by_email(db, email)

        if not user:
            raise ValueError("Could not validate credentials!")
        
        if not check_hashed_pwd(password, user.password):
            raise ValueError("Could not validate credentials!")
        
        token = create_access_token(data={"sub": user.id})

        return {
        "access_token": token,
        "token_type": "bearer"
        }
    
LoginUseCase = LoginUseCase()


