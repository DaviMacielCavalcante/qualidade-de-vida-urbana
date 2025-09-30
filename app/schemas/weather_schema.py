from pydantic import BaseModel, Field

class WeatherSchema(BaseModel):
    id: int
    placeId: int = Field(..., gt=0)
    sourceId: int = Field(..., gt=0)
    temperature: float = Field(..., max_digits=5, decimal_places=2)
    humidity: float = Field(...)
    windSpeed: float = Field(..., max_digits=4, decimal_places=2)
    windDirection: int = Field(...)
    createdAt: str = Field(...)