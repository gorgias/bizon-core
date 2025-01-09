from pydantic import BaseModel


class SlackConfig(BaseModel):
    webhook_url: str
