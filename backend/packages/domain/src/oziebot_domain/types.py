from pydantic import BaseModel, ConfigDict


class OziebotModel(BaseModel):
    """Base model: strict, immutable value objects."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)
