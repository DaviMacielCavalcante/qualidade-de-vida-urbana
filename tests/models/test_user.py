"""
Models de teste (cópias dos models de produção sem schema PostgreSQL).

ATENÇÃO: Se modificar app/models/user_model.py, atualizar aqui também!
"""

from sqlalchemy import Column, String, DateTime, Enum
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func
import uuid

from app.schemas.enums import (
    UserNotificationPreferenceEnum,
    UserRoleEnum,
    UserSignatureStatusEnum
)

# Base para models de teste
class TestBase(DeclarativeBase):
    pass

class User(TestBase):
    """
    Cópia de app.models.user_model.User sem schema PostgreSQL.
    Usado apenas em testes com SQLite.
    """
    __tablename__ = 'users'
    # SEM: __table_args__ = {'schema': 'dev'}
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(100), index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    password = Column(String(255), nullable=False)
    

    signatureStatus = Column(
        Enum(UserSignatureStatusEnum, name="usersignaturestatusenum"),
        nullable=True
    )
    role = Column(
        Enum(UserRoleEnum, name="userroleenum"),
        nullable=True
    )
    notifications = Column(
        Enum(UserNotificationPreferenceEnum, name="usernotificationpreferenceenum"),
        nullable=True
    )
    
    phone_number = Column(String(20), nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, onupdate=func.now(), nullable=True)