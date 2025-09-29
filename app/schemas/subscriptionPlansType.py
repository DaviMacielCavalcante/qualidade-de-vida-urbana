from pydantic import BaseModel, Field
from decimal import Decimal

class subscriptionPlansTypeSchema(BaseModel):
    id: int
    name: str = Field(..., min_length=1, max_length=100)
    description: str 
    daysTimeDuration: str 
    price: Decimal = Field(..., gt=Decimal(0))