from pydantic import BaseModel, Field
from datetime import datetime
from enums import SubscriptionStatusEnum

class SubscriptionTypeSchema(BaseModel):
    id: int
    userId: str = Field(..., min_length=1)
    placeId: int = Field(..., gt=0)
    startDate: datetime = Field(...)
    endDate: datetime = Field(...)
    status: SubscriptionStatusEnum = Field(...)
    createdAt: datetime 
    updatedAt: datetime 
