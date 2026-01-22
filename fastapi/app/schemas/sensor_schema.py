from pydantic import BaseModel, Field
from datetime import datetime

class SensorSchema(BaseModel):
    id: int = Field(..., gt=0)
    place_id: int = Field(..., gt=0, alias="placeId")
    source_id: int = Field(..., gt=0, alias="sourceId")
    water_level: float = Field(..., max_digits=5, decimal_places=2, alias="waterLevel") 
    water_level_unit: str = Field(..., min_length=1, max_length=20, alias="waterLevelUnit")
    created_at: datetime = Field(..., alias="createdAt")