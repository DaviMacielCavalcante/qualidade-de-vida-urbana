from sqlalchemy.orm import Session 
from passlib.context import CryptContext
from typing import List
from app.models.user_model import User
from app.schemas.user_schema import UserCreate, UserUpdate

pwd_context = CryptContext(schemes=["argon2"])

def get_pwd_hash(pwd: str) -> str:
    return pwd_context.hash(pwd)

def check_hashed_pwd(pwd: str, hashed_pwd: str) -> bool:
    return pwd_context.verify(pwd, hashed_pwd)

class UserRepository:

    def create_user(self, db: Session, user: UserCreate) -> User:

        user_db = User(
            name=user.name,
            email=user.email,
            phone_number=user.phone_number,
            role=user.role,
            password=get_pwd_hash(user.password),
            signatureStatus=user.signatureStatus,
            notifications=user.notifications
        )

        db.add(user_db)
        db.commit()
        db.refresh(user_db)
        return user_db
    
    def get_user_by_id(self, db: Session, user_id: str) -> User | None:
        return db.query(User).filter(User.id == user_id).first()
    
    def get_user_by_email(self, db: Session, email: str) -> User | None:
        return db.query(User).filter(User.email == email).first()
    
    def get_all_users(self, db: Session, skip: int = 0, limit: int = 100) -> List[User]:
        return db.query(User).offset(skip).limit(limit).all()
    
    def update_user(self, db: Session, user_obj: User, update_obj: UserUpdate) -> User | None:
        
        update_data = update_obj.model_dump(exclude_unset=True)

        if not update_data:
            return None
        
        if "password" in update_data:
            setattr(user_obj, "password", get_pwd_hash(update_data["password"]))   
            del update_data["password"]
        
        for key, value in update_data.items():
            setattr(user_obj, key, value)
        
        db.commit()
        db.refresh(user_obj)
        return user_obj
    
    def delete_user(self, db: Session, email: str) -> User | None:

        user_db = self.get_user_by_email(db, email)

        if not user_db:
            return None
        
        db.delete(user_db)
        db.commit()
        return user_db
        

user_repository = UserRepository()
    

