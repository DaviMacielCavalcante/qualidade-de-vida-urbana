from pydantic import BaseModel, Field, EmailStr
from datetime import datetime
from enums import UserNotificationPreferenceEnum, UserRoleEnum, UserSignatureStatusEnum

class UserSchema(BaseModel):
    id: str
    name: str = Field(..., min_length=1, max_length=100)
    email: EmailStr = Field(...)
    password: str = Field(..., min_length=8, max_length=255)
    signatureStatus: UserSignatureStatusEnum | None
    phoneNumber: str = Field(..., min_length=11, max_length=20)
    role: UserRoleEnum | None
    notifications: UserNotificationPreferenceEnum | None
    createdAt: datetime = Field(...)
    updatedAt: datetime | None
