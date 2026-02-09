"""Shared data models for Federal Reserve documents.

This module defines the core data structures used throughout the ledger
for representing Federal Reserve communications with normalized metadata.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List


def _default_timestamp() -> datetime:
    """Generate a default UTC timestamp.
    
    Returns UTC-aware datetime for Python 3.11+ or naive UTC datetime for earlier versions.
    
    Returns:
        Current UTC timestamp.
    """
    if hasattr(datetime, 'UTC'):
        return datetime.now(datetime.UTC)
    return datetime.utcnow()


@dataclass
class Document:
    """Base representation of a Federal Reserve document.
    
    This class captures the core metadata shared across all document types,
    maintaining a clear separation between source preservation (raw data)
    and extracted information (parsed metadata).
    
    Attributes:
        doc_id: Stable 16-character identifier derived from source_url.
        source_url: Original URL where the document was retrieved.
        fetch_timestamp: UTC timestamp when the document was fetched.
        raw_path: Path to the raw saved content (HTML or PDF).
        content_type: MIME type of the raw content (e.g., "text/html", "application/pdf").
        title: Document title, if available.
        published_date: Publication date of the document, if available.
        doc_type: Type of document (e.g., "statement", "minutes", "speech").
        metadata: Additional metadata specific to the document type.
    """
    
    doc_id: str
    source_url: str
    fetch_timestamp: datetime
    raw_path: str
    content_type: str
    title: Optional[str] = None
    published_date: Optional[datetime] = None
    doc_type: Optional[str] = None
    metadata: dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate document fields after initialization."""
        if not self.doc_id or len(self.doc_id) != 16:
            raise ValueError(f"doc_id must be 16 characters, got: {self.doc_id}")
        
        if not self.source_url:
            raise ValueError("source_url is required")
        
        if not self.raw_path:
            raise ValueError("raw_path is required")


@dataclass
class FOMCStatement(Document):
    """FOMC (Federal Open Market Committee) statement document.
    
    Extends the base Document class with fields specific to FOMC statements.
    
    Attributes:
        meeting_date: Date of the FOMC meeting.
        policy_decision: Key policy decision text, if extracted.
        vote_summary: Summary of committee voting, if available.
    """
    
    meeting_date: Optional[datetime] = None
    policy_decision: Optional[str] = None
    vote_summary: Optional[str] = None


@dataclass
class Speech:
    """Federal Reserve official speech or testimony.
    
    Represents speeches, testimonies, and public remarks by Federal Reserve
    officials.
    
    Attributes:
        doc_id: Stable document identifier.
        source_url: Original URL of the speech.
        speaker: Name of the speaker.
        speaker_title: Official title of the speaker.
        event_name: Name of the event where speech was delivered.
        location: Location of the speech.
        speech_date: Date the speech was delivered.
        raw_path: Path to saved raw content.
        metadata: Additional metadata.
    """
    
    doc_id: str
    source_url: str
    speaker: str
    speaker_title: Optional[str] = None
    event_name: Optional[str] = None
    location: Optional[str] = None
    speech_date: Optional[datetime] = None
    raw_path: Optional[str] = None
    metadata: dict = field(default_factory=dict)


@dataclass
class FetchResult:
    """Result of a document fetch operation.
    
    Captures the outcome of attempting to retrieve a document, including
    success status, saved paths, and any errors encountered.
    
    Attributes:
        success: Whether the fetch was successful.
        doc_id: Document identifier.
        source_url: URL that was fetched.
        raw_path: Path where raw content was saved, if successful.
        error: Error message, if fetch failed.
        status_code: HTTP status code from the fetch.
        content_type: MIME type of the fetched content.
        timestamp: When the fetch was attempted.
    """
    
    success: bool
    doc_id: str
    source_url: str
    raw_path: Optional[str] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    content_type: Optional[str] = None
    timestamp: datetime = field(default_factory=_default_timestamp)
