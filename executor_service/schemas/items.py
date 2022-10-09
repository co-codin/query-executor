from pydantic import BaseModel


class ItemIn(BaseModel):
    id: int
