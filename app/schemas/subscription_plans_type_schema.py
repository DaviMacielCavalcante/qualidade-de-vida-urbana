from pydantic import BaseModel, Field
from decimal import Decimal

class SubscriptionPlansTypeSchema(BaseModel):
    id: int
    name: str = Field(..., min_length=1, max_length=100)
    description: str = Field(...)
    daysTimeDuration: str = Field(...)
    price: Decimal = Field(..., gt=Decimal(0))