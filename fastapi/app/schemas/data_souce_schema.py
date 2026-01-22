from pydantic import BaseModel, Field

class DataSourceSchema(BaseModel):
    id: int = Field(..., gt=0)
    name: str = Field(...)