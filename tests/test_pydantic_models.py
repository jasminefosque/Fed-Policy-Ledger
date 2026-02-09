"""Test Pydantic models."""

import pytest
from datetime import datetime
from fedledger.pydantic_models import (
    BaseDocument,
    FOMCStatementModel,
    SpeechModel,
    DocumentType,
)


def test_base_document_creation():
    """Test creating a base document."""
    doc = BaseDocument(
        doc_id="abc1234567890def",
        source_url="https://www.federalreserve.gov/test.htm",
        fetch_timestamp=datetime.utcnow(),
        raw_path="data/raw/abc1234567890def.html",
        content_type="text/html",
        doc_type=DocumentType.STATEMENT,
        title="Test Document",
    )
    
    assert doc.doc_id == "abc1234567890def"
    assert doc.title == "Test Document"
    assert doc.doc_type == DocumentType.STATEMENT


def test_doc_id_validation():
    """Test doc_id validation."""
    from pydantic import ValidationError
    
    # Valid doc_id
    doc = BaseDocument(
        doc_id="abc1234567890def",
        source_url="https://test.com",
        fetch_timestamp=datetime.utcnow(),
        raw_path="test.html",
        content_type="text/html",
        doc_type=DocumentType.STATEMENT,
    )
    assert doc.doc_id == "abc1234567890def"
    
    # Invalid length
    with pytest.raises(ValidationError, match="at least 16 characters"):
        BaseDocument(
            doc_id="abc123",
            source_url="https://test.com",
            fetch_timestamp=datetime.utcnow(),
            raw_path="test.html",
            content_type="text/html",
            doc_type=DocumentType.STATEMENT,
        )
    
    # Invalid hex
    with pytest.raises(ValueError, match="doc_id must be hexadecimal"):
        BaseDocument(
            doc_id="xyz1234567890xyz",
            source_url="https://test.com",
            fetch_timestamp=datetime.utcnow(),
            raw_path="test.html",
            content_type="text/html",
            doc_type=DocumentType.STATEMENT,
        )


def test_fomc_statement_model():
    """Test FOMC statement model."""
    stmt = FOMCStatementModel(
        doc_id="abc1234567890def",
        source_url="https://www.federalreserve.gov/statement.htm",
        fetch_timestamp=datetime.utcnow(),
        raw_path="data/raw/abc1234567890def.html",
        content_type="text/html",
        meeting_date=datetime(2024, 1, 31),
        policy_decision="Maintain rates at 5.25-5.50%",
        participants=["Jerome Powell", "John Williams"],
    )
    
    assert stmt.doc_type == DocumentType.STATEMENT
    assert stmt.meeting_date == datetime(2024, 1, 31)
    assert len(stmt.participants) == 2


def test_speech_model():
    """Test speech model."""
    speech = SpeechModel(
        doc_id="abc1234567890def",
        source_url="https://www.federalreserve.gov/speech.htm",
        fetch_timestamp=datetime.utcnow(),
        raw_path="data/raw/abc1234567890def.html",
        content_type="text/html",
        speaker="Jerome Powell",
        speaker_title="Chair",
        event_name="Economic Club of New York",
    )
    
    assert speech.doc_type == DocumentType.SPEECH
    assert speech.speaker == "Jerome Powell"
    assert speech.speaker_title == "Chair"


def test_to_parquet_row():
    """Test conversion to Parquet row format."""
    doc = BaseDocument(
        doc_id="abc1234567890def",
        source_url="https://test.com",
        fetch_timestamp=datetime.utcnow(),
        raw_path="test.html",
        content_type="text/html",
        doc_type=DocumentType.STATEMENT,
        title="Test Document",
    )
    
    row = doc.to_parquet_row()
    
    assert row["doc_id"] == "abc1234567890def"
    assert row["source_url"] == "https://test.com"
    assert row["title"] == "Test Document"
    assert row["doc_type"] == "statement"


def test_to_json_metadata():
    """Test conversion to JSON metadata format."""
    doc = BaseDocument(
        doc_id="abc1234567890def",
        source_url="https://test.com",
        fetch_timestamp=datetime.utcnow(),
        raw_path="test.html",
        content_type="text/html",
        doc_type=DocumentType.STATEMENT,
        title="Test Document",
    )
    
    metadata = doc.to_json_metadata()
    
    assert metadata["doc_id"] == "abc1234567890def"
    assert metadata["title"] == "Test Document"
    assert isinstance(metadata, dict)
