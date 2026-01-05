from pydantic import BaseModel, Field


class TransformModel(BaseModel):
    label: str = Field(..., description="Label of the transform")
    python: str = Field(..., description="Python code for the transform")
