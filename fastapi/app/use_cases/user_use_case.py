from sqlalchemy.orm import Session
from app.models.user_model import User
from app.repositories.user_repository import UserRepository, user_repository
from app.schemas.user_schema import UserCreate, UserUpdate

class CreateUserUseCase:

    def __init__(self, repository: UserRepository = user_repository):
        self.repository = repository
        
    def execute(self, db: Session, user_create: UserCreate) -> User:
        
       is_email_already_used = self.repository.get_user_by_email(db, user_create.email)

       if is_email_already_used:
        raise ValueError("User with this email already exists")
       
       return self.repository.create_user(db, user_create)
    
class GetUserByEmailUseCase:
   
    def __init__(self, repository: UserRepository = user_repository):
        self.repository = repository
        
    def execute(self, db: Session, email: str) -> User | None:
    
        user = self.repository.get_user_by_email(db, email)

        if not user:
            raise ValueError(f"User with email {email} not found")

        return user
    
class GetAllUsersUseCase:
   
    def __init__(self, repository: UserRepository = user_repository):
        self.repository = repository
        
    def execute(self, db: Session, skip: int = 0, limit: int = 100) -> list[User]:

        if skip < 0 or limit < 1:
            raise ValueError("Skip must be non-negative and limit must be positive")
        return self.repository.get_all_users(db, skip, limit)
    
class UpdateUserUseCase:

    def __init__(self, repository: UserRepository = user_repository):
        self.repository = repository

    def execute(self, db: Session, email: str, user_update: UserUpdate) -> User:
        
        user = self.repository.get_user_by_email(db, email)

        if not user:
            raise ValueError(f"User with email {email} not found")

        if user_update.email and user_update.email != user.email:
            is_email_used = self.repository.get_user_by_email(db, user_update.email)
            if is_email_used:
                raise ValueError(f"Email {email} already used")

        updated_user = self.repository.update_user(db, user, user_update)

        if not updated_user:
            raise ValueError("No fields to Update")

        return updated_user
    
class DeleteUserByEmailUseCase:

    def __init__(self, repository: UserRepository = user_repository):
        self.repository = repository
        
    def execute(self, db: Session, email: str) -> User | None:
    
        user = self.repository.get_user_by_email(db, email)

        if not user:
            raise ValueError(f"User with email {email} not found")

        self.repository.delete_user(db, email)

        return user
    


CreateUserUseCase = CreateUserUseCase()
GetUserByEmailUseCase = GetUserByEmailUseCase()
GetAllUsersUseCase = GetAllUsersUseCase()
UpdateUserUseCase = UpdateUserUseCase()
DeleteUserByEmailUseCase = DeleteUserByEmailUseCase()
       
