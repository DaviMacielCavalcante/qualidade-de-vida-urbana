from pydantic import BaseModel, Field
from datetime import datetime
from enums import SubscriptionStatusEnum

class SubscriptionTypeSchema(BaseModel):
    id: int
    user_id: str = Field(..., min_length=1, alias="userId")
    place_id: int = Field(..., gt=0, alias="placeId")
    start_date: datetime = Field(..., alias="startDate")
    end_date: datetime = Field(..., alias="endDate")
    status: SubscriptionStatusEnum = Field(...)
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt") 
