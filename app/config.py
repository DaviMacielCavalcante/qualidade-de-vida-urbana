import os
from functools import lru_cache

class Settings:

    def __init__(self):
        self.secret_key = os.getenv("SECRET_KEY")
        if not self.secret_key:
            raise ValueError("SECRET_KEY env not defined!")
        
        self.algorithm = os.getenv("ALGORITHM")

        if not self.algorithm:
            raise ValueError("ALGORITHM env not defined!")
        
        self.access_token_expire_minutes = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))

@lru_cache
def get_settings():

    return Settings()