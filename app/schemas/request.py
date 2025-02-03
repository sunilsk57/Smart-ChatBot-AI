from pydantic import BaseModel


class UserRequest:
    name: str
    email: str
    password: str
