from pydantic import BaseModel, Field

class SendMessageSchema(BaseModel):
    to: str = Field(..., min_length=10, max_length=15, description="Phone number with country code (e.g., +559188772828)")
    place: str = Field(..., min_length=1, description="Location Name")
    floor: str = Field(..., min_length=1, description="Water level")