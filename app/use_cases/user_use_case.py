from sqlalchemy.orm import Session
from app.models.user_model import User
from app.repositories.user_repository import UserRepository, user_repository
from app.schemas.user_schema import UserCreate

class CreateUserUseCase:

    def __init__(self, repository: UserRepository = user_repository):
        self.repository = repository
        
    def execute(self, db: Session, user_create: UserCreate) -> User:
        
       is_email_already_used = self.repository.get_user_by_email(db, user_create.email)

       if is_email_already_used:
        raise ValueError("User with this email already exists")
       

CreateUserUseCase = CreateUserUseCase()
       
