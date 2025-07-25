from pydantic import BaseModel

class APIClientInputConfiguration(BaseModel):
    """
    Configuration for API client input.
    """
    
    base_url: str
    api_key: str
    # city: dict
    units: str = "metric"
    language: str = "en"