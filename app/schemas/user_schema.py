from pydantic import BaseModel, Field, EmailStr, ConfigDict
from datetime import datetime
from .enums import UserNotificationPreferenceEnum, UserRoleEnum, UserSignatureStatusEnum

class UserSchema(BaseModel):

    model_config = ConfigDict(populate_by_name=True, from_attributes=True)

    id: str
    name: str = Field(..., min_length=1, max_length=100)
    email: EmailStr = Field(...)
    signatureStatus: UserSignatureStatusEnum | None
    phone_number: str = Field(..., min_length=11, max_length=20, alias="phoneNumber")
    role: UserRoleEnum | None
    notifications: UserNotificationPreferenceEnum | None
class UserCreate(UserSchema):
    password: str = Field(..., min_length=8, max_length=255)

class UserRead(BaseModel):
    
    model_config = ConfigDict(populate_by_name=True, from_attributes=True)
    
    name: str
    email: EmailStr
    signatureStatus: UserSignatureStatusEnum | None = None
    phone_number: str = Field(..., alias="phoneNumber")
    role: UserRoleEnum | None = None
    notifications: UserNotificationPreferenceEnum | None = None
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime | None = Field(None, alias="updatedAt")


class UserUpdate(BaseModel):

    model_config = ConfigDict(populate_by_name=True)
    

    name: str | None = None
    email: EmailStr | None = None
    phone_number: str | None = Field(None, min_length=11, max_length=20, alias="phoneNumber")
    password: str | None = Field(None, min_length=8, max_length=255)
    signatureStatus: UserSignatureStatusEnum | None = None
    role: UserRoleEnum | None = None
    notifications: UserNotificationPreferenceEnum | None = None