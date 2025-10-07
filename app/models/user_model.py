from sqlalchemy import Column, String, DateTime, Enum
from sqlalchemy.sql import func
from app.database import Base
from app.schemas.enums import UserNotificationPreferenceEnum, UserRoleEnum, UserSignatureStatusEnum
import uuid

class User(Base):
    __tablename__ = 'users'
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(100), index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    password = Column(String(255), nullable=False)
    signatureStatus = Column(Enum(UserSignatureStatusEnum), nullable=True)
    phone_number = Column(String(20), nullable=False)
    role = Column(Enum(UserRoleEnum), nullable=True)
    notifications = Column(Enum(UserNotificationPreferenceEnum), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, onupdate=func.now(), nullable=True)