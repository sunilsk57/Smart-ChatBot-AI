from pydantic import BaseModel


class UserRequest(BaseModel):
    name: str
    email: str
    password: str
