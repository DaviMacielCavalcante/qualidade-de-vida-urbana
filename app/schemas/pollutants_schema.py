from pydantic import BaseModel, Field
from datetime import datetime

class PollutantSchema(BaseModel):
    id: int = Field(..., gt=0)
    place_id: int = Field(..., gt=0, alias="placeId")
    data_source_id: int = Field(..., gt=0, alias="dataSourceId")
    dominant_pollutant: str = Field(..., min_length=1, max_length=50, alias="dominantPollutant")
    concentration: float = Field(..., max_digits=7, decimal_places=4)
    metric_unit: str = Field(..., min_length=1, max_length=20, alias="metricUnit")
    created_at: datetime = Field(..., alias="createdAt")