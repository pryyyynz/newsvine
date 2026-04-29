from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, EmailStr, Field, model_validator


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    name: str | None = Field(default=None, min_length=1, max_length=255)
    country: str | None = Field(default=None, max_length=8)


class RegisterResponse(BaseModel):
    id: int
    email: EmailStr
    name: str | None = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenPairResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshRequest(BaseModel):
    refresh_token: str


class NewsArticle(BaseModel):
    id: str
    title: str
    content: str
    content_snippet: str | None = None
    category: str
    timestamp: str
    source: str
    country: str
    url: str
    image_url: str | None = None


class NewsListResponse(BaseModel):
    total: int
    items: list[NewsArticle]


EventType = Literal["click", "like", "bookmark", "search", "time_spent"]


class EventMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")

    user_id: str | None = None
    country: str | None = None
    topic: str | None = None


class EventRequest(BaseModel):
    event_type: EventType
    article_id: str = Field(min_length=1, max_length=128)
    query: str | None = None
    metadata: EventMetadata = Field(default_factory=EventMetadata)

    @model_validator(mode="after")
    def validate_search_query(self) -> "EventRequest":
        if self.event_type == "search" and not self.query:
            raise ValueError("query is required when event_type is search")
        return self


class EventAcceptedResponse(BaseModel):
    status: str
    event_id: str


class TrendingItem(BaseModel):
    score: float
    article: NewsArticle


class TrendingListResponse(BaseModel):
    total: int
    items: list[TrendingItem]


class RecommendationItem(BaseModel):
    score: float
    article: NewsArticle


class RecommendationListResponse(BaseModel):
    total: int
    items: list[RecommendationItem]


class ErrorResponse(BaseModel):
    error: str
    message: str
    code: str


class SearchResultItem(BaseModel):
    relevance_score: float
    article: NewsArticle


class SearchResponse(BaseModel):
    total: int
    items: list[SearchResultItem]


class UserProfileResponse(BaseModel):
    id: int
    email: EmailStr
    name: str | None = None
    country: str
    preferences: dict[str, Any]


class UpdateUserProfileRequest(BaseModel):
    country: str | None = Field(default=None, min_length=2, max_length=8)
    name: str | None = Field(default=None, max_length=255)
    preferences: dict[str, Any] = Field(default_factory=dict)


class UserHistoryItem(BaseModel):
    article_id: str
    read_at: str
    article: NewsArticle | None = None


class UserHistoryResponse(BaseModel):
    total: int
    items: list[UserHistoryItem]


class UserBookmarkItem(BaseModel):
    article_id: str
    created_at: str


class UserBookmarksResponse(BaseModel):
    total: int
    items: list[UserBookmarkItem]
