from pydantic import BaseModel, Field
from datetime import datetime

class AirQualitySchema(BaseModel):
    id: int = Field(..., gt=0)
    place_id: int = Field(..., gt=0)
    source_id: int = Field(..., gt=0)
    aqi_universal: float = Field(..., ge=0, alias="aqiUniversal")
    name_display: str = Field(..., min_length=1, alias="nameDisplay")
    display_value: str = Field(..., min_length=1, alias="displayValue")
    created_at: datetime = Field(..., alias="createdAt")