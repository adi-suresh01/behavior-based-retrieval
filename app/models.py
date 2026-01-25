from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ConfigDict


class SlackReaction(BaseModel):
    name: str
    count: int = 1


class SlackInnerEvent(BaseModel):
    model_config = ConfigDict(extra="allow")
    type: str
    channel: Optional[str] = None
    user: Optional[str] = None
    text: Optional[str] = None
    ts: Optional[str] = None
    thread_ts: Optional[str] = None
    reactions: Optional[List[SlackReaction]] = None
    subtype: Optional[str] = None
    message: Optional[Dict[str, Any]] = None
    previous_message: Optional[Dict[str, Any]] = None
    item: Optional[Dict[str, Any]] = None
    reaction: Optional[str] = None


class SlackEventPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    event_id: str
    event_time: Optional[int] = None
    event_ts: Optional[str] = None
    team_id: Optional[str] = None
    type: str
    event: SlackInnerEvent


class IngestResult(BaseModel):
    status: str
    event_id: str


class ThreadView(BaseModel):
    thread_ts: str
    channel: str
    root_ts: str
    created_at: float
    last_activity: float
    reply_count: int
    reaction_count: int
    participants: List[str] = Field(default_factory=list)


class DigestItemView(BaseModel):
    thread_ts: str
    channel: str
    title: Optional[str]
    labels: List[str] = Field(default_factory=list)
    entities: dict = Field(default_factory=dict)
    urgency: float
    summary: Optional[str]
    updated_at: float


class EmbeddingView(BaseModel):
    thread_ts: str
    dim: int
    vector: List[float]
    updated_at: float


class QueueStatus(BaseModel):
    name: str
    size: int
    processed_count: int
    last_processed_at: Optional[float] = None


class HealthResponse(BaseModel):
    status: str
