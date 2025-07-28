from pydantic import BaseModel, Field


class BaseHandoff(BaseModel):
    """Shared base class for all agent handoff payloads.

    This ensures every handoff includes the user's original request while still
    allowing subclasses to introduce additional, task-specific fields.
    """

    original_request: str = Field(
        ..., description="The full original user request, without any modifications."
    )
