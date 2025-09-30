from pydantic import BaseModel, Field
from datetime import datetime
class WeatherSchema(BaseModel):
    id: int
    place_id: int = Field(..., gt=0, alias="placeId")
    source_id: int = Field(..., gt=0, alias="sourceId")
    temperature: float = Field(..., max_digits=5, decimal_places=2)
    humidity: float = Field(...)
    wind_speed: float = Field(..., max_digits=4, decimal_places=2, alias="windSpeed")
    wind_direction: int = Field(..., alias="windDirection")
    created_at: datetime = Field(..., alias="createdAt")