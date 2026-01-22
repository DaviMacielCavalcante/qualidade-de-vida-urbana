from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class FavoritePlaceSchema(BaseModel):
    user_id: int = Field(..., gt=0, alias="userId")
    place_id: int = Field(..., gt=0, alias="placeId")
    created_at: datetime = Field(..., alias="createdAt")
    name: Optional[str] 