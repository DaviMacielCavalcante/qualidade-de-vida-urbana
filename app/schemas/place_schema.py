from pydantic import BaseModel, Field
from datetime import datetime

class PlaceSchema(BaseModel):
    id: int = Field(..., gt=0)
    name: str = Field(..., min_length=1, max_length=100)
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")