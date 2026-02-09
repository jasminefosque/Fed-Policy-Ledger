"""Pydantic models for Fed Policy Ledger.

This module provides type-safe, validated Pydantic models for all document types
with conversion methods for different data formats.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path
from pydantic import BaseModel, Field, field_validator, ConfigDict
from enum import Enum


class DocumentType(str, Enum):
    """Valid document types."""
    STATEMENT = "statement"
    MINUTES = "minutes"
    SPEECH = "speech"
    PRESS_CONFERENCE = "press_conference"
    TESTIMONY = "testimony"
    REPORT = "report"


class BaseDocument(BaseModel):
    """Base Pydantic model for all Federal Reserve documents.
    
    Provides validation, normalization, and conversion methods for
    document metadata.
    
    Attributes:
        doc_id: Stable 16-character identifier derived from source_url.
        source_url: Original URL where the document was retrieved.
        fetch_timestamp: UTC timestamp when the document was fetched.
        raw_path: Path to the raw saved content (HTML or PDF).
        content_type: MIME type of the raw content.
        title: Document title.
        published_date: Publication date of the document.
        doc_type: Type of document.
        metadata: Additional metadata specific to the document type.
    """
    
    doc_id: str = Field(..., min_length=16, max_length=16, description="Stable 16-char hex ID")
    source_url: str = Field(..., description="Original source URL")
    fetch_timestamp: datetime = Field(..., description="When document was fetched")
    raw_path: str = Field(..., description="Path to raw content")
    content_type: str = Field(..., description="MIME type")
    title: Optional[str] = Field(None, description="Document title")
    published_date: Optional[datetime] = Field(None, description="Publication date")
    doc_type: DocumentType = Field(..., description="Document type")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    model_config = ConfigDict(
        use_enum_values=True,
        validate_assignment=True,
        str_strip_whitespace=True,
    )
    
    @field_validator("doc_id")
    @classmethod
    def validate_doc_id(cls, v: str) -> str:
        """Validate doc_id is 16-character hex string."""
        if not v or len(v) != 16:
            raise ValueError(f"doc_id must be 16 characters, got: {v}")
        try:
            int(v, 16)
        except ValueError:
            raise ValueError(f"doc_id must be hexadecimal, got: {v}")
        return v.lower()
    
    @field_validator("source_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate source URL is not empty."""
        if not v or not v.strip():
            raise ValueError("source_url cannot be empty")
        return v.strip()
    
    def to_parquet_row(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for Parquet storage.
        
        Returns:
            Dictionary with flattened fields for Parquet row.
        """
        row = {
            "doc_id": self.doc_id,
            "source_url": self.source_url,
            "fetch_timestamp": self.fetch_timestamp,
            "raw_path": self.raw_path,
            "content_type": self.content_type,
            "title": self.title,
            "published_date": self.published_date,
            "doc_type": self.doc_type,
        }
        
        # Flatten metadata into columns with prefix
        for key, value in self.metadata.items():
            row[f"meta_{key}"] = value
        
        return row
    
    def to_json_metadata(self) -> Dict[str, Any]:
        """Convert to JSON-serializable metadata dictionary.
        
        Returns:
            Dictionary suitable for JSON serialization.
        """
        return self.model_dump(mode="json")
    
    @classmethod
    def from_raw_metadata(cls, metadata: Dict[str, Any]) -> "BaseDocument":
        """Create instance from raw metadata dictionary.
        
        Args:
            metadata: Raw metadata dictionary.
        
        Returns:
            Validated BaseDocument instance.
        """
        return cls(**metadata)


class FOMCStatementModel(BaseDocument):
    """Pydantic model for FOMC statements.
    
    Extends BaseDocument with FOMC-specific fields.
    
    Attributes:
        meeting_date: Date of the FOMC meeting.
        policy_decision: Key policy decision text.
        vote_summary: Summary of committee voting.
        participants: List of committee participants.
    """
    
    doc_type: DocumentType = Field(default=DocumentType.STATEMENT)
    meeting_date: Optional[datetime] = Field(None, description="FOMC meeting date")
    policy_decision: Optional[str] = Field(None, description="Policy decision text")
    vote_summary: Optional[str] = Field(None, description="Voting summary")
    participants: List[str] = Field(default_factory=list, description="Committee participants")
    
    def to_parquet_row(self) -> Dict[str, Any]:
        """Convert to Parquet row with FOMC-specific fields."""
        row = super().to_parquet_row()
        row.update({
            "meeting_date": self.meeting_date,
            "policy_decision": self.policy_decision,
            "vote_summary": self.vote_summary,
            "participants": ",".join(self.participants) if self.participants else None,
        })
        return row
    
    @classmethod
    def from_html(cls, html_content: str, doc_id: str, source_url: str, raw_path: str) -> "FOMCStatementModel":
        """Create instance from HTML content.
        
        Args:
            html_content: Raw HTML content.
            doc_id: Document ID.
            source_url: Source URL.
            raw_path: Path to raw file.
        
        Returns:
            Parsed FOMCStatementModel instance.
        
        Note:
            This is a placeholder. Actual HTML parsing logic should be implemented.
        """
        # Placeholder for HTML parsing
        return cls(
            doc_id=doc_id,
            source_url=source_url,
            fetch_timestamp=datetime.utcnow(),
            raw_path=raw_path,
            content_type="text/html",
            doc_type=DocumentType.STATEMENT,
        )


class FOMCMinutesModel(BaseDocument):
    """Pydantic model for FOMC minutes.
    
    Attributes:
        meeting_date: Date of the FOMC meeting.
        participants: List of committee participants.
        economic_projections: Summary of economic projections.
    """
    
    doc_type: DocumentType = Field(default=DocumentType.MINUTES)
    meeting_date: Optional[datetime] = Field(None, description="FOMC meeting date")
    participants: List[str] = Field(default_factory=list, description="Committee participants")
    economic_projections: Optional[str] = Field(None, description="Economic projections")
    
    def to_parquet_row(self) -> Dict[str, Any]:
        """Convert to Parquet row with minutes-specific fields."""
        row = super().to_parquet_row()
        row.update({
            "meeting_date": self.meeting_date,
            "participants": ",".join(self.participants) if self.participants else None,
            "economic_projections": self.economic_projections,
        })
        return row


class SpeechModel(BaseDocument):
    """Pydantic model for Federal Reserve speeches.
    
    Attributes:
        speaker: Name of the speaker.
        speaker_title: Official title of the speaker.
        event_name: Name of the event.
        location: Location of the speech.
        speech_date: Date the speech was delivered.
    """
    
    doc_type: DocumentType = Field(default=DocumentType.SPEECH)
    speaker: Optional[str] = Field(None, description="Speaker name")
    speaker_title: Optional[str] = Field(None, description="Speaker title")
    event_name: Optional[str] = Field(None, description="Event name")
    location: Optional[str] = Field(None, description="Location")
    speech_date: Optional[datetime] = Field(None, description="Speech date")
    
    def to_parquet_row(self) -> Dict[str, Any]:
        """Convert to Parquet row with speech-specific fields."""
        row = super().to_parquet_row()
        row.update({
            "speaker": self.speaker,
            "speaker_title": self.speaker_title,
            "event_name": self.event_name,
            "location": self.location,
            "speech_date": self.speech_date,
        })
        return row
    
    @classmethod
    def from_html(cls, html_content: str, doc_id: str, source_url: str, raw_path: str) -> "SpeechModel":
        """Create instance from HTML content.
        
        Args:
            html_content: Raw HTML content.
            doc_id: Document ID.
            source_url: Source URL.
            raw_path: Path to raw file.
        
        Returns:
            Parsed SpeechModel instance.
        
        Note:
            This is a placeholder. Actual HTML parsing logic should be implemented.
        """
        # Placeholder for HTML parsing
        return cls(
            doc_id=doc_id,
            source_url=source_url,
            fetch_timestamp=datetime.utcnow(),
            raw_path=raw_path,
            content_type="text/html",
            doc_type=DocumentType.SPEECH,
        )


class PressConferenceModel(BaseDocument):
    """Pydantic model for press conference transcripts.
    
    Attributes:
        meeting_date: Date of the associated FOMC meeting.
        chair_name: Name of the Fed Chair.
        participants: List of participants.
    """
    
    doc_type: DocumentType = Field(default=DocumentType.PRESS_CONFERENCE)
    meeting_date: Optional[datetime] = Field(None, description="FOMC meeting date")
    chair_name: Optional[str] = Field(None, description="Fed Chair name")
    participants: List[str] = Field(default_factory=list, description="Participants")
    
    def to_parquet_row(self) -> Dict[str, Any]:
        """Convert to Parquet row with press conference-specific fields."""
        row = super().to_parquet_row()
        row.update({
            "meeting_date": self.meeting_date,
            "chair_name": self.chair_name,
            "participants": ",".join(self.participants) if self.participants else None,
        })
        return row


# Type alias for any document model
DocumentModel = BaseDocument | FOMCStatementModel | FOMCMinutesModel | SpeechModel | PressConferenceModel
