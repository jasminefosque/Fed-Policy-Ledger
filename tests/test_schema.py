"""Test schema validation."""

import pytest
from datetime import datetime
from fedledger.schema import (
    get_schema_for_doc_type,
    validate_row,
    validate_rows,
    VALID_DOC_TYPES,
)


def test_get_schema_for_doc_type():
    """Test schema retrieval for different document types."""
    schema = get_schema_for_doc_type("statement")
    assert schema is not None
    assert "doc_id" in schema.names
    assert "meeting_date" in schema.names
    
    schema = get_schema_for_doc_type("speech")
    assert schema is not None
    assert "speaker" in schema.names


def test_invalid_doc_type():
    """Test error on invalid document type."""
    with pytest.raises(ValueError, match="Invalid doc_type"):
        get_schema_for_doc_type("invalid_type")


def test_validate_row_success():
    """Test successful row validation."""
    schema = get_schema_for_doc_type("statement")
    
    row = {
        "doc_id": "abc1234567890def",
        "source_url": "https://test.com",
        "fetch_timestamp": datetime.utcnow(),
        "raw_path": "test.html",
        "content_type": "text/html",
        "doc_type": "statement",
    }
    
    # Should not raise
    validate_row(row, schema)


def test_validate_row_missing_required():
    """Test validation fails on missing required field."""
    schema = get_schema_for_doc_type("statement")
    
    row = {
        "doc_id": "abc1234567890def",
        # Missing source_url
        "fetch_timestamp": datetime.utcnow(),
        "raw_path": "test.html",
        "content_type": "text/html",
        "doc_type": "statement",
    }
    
    with pytest.raises(ValueError, match="Required field missing"):
        validate_row(row, schema)


def test_validate_row_invalid_doc_type():
    """Test validation fails on invalid doc_type value."""
    schema = get_schema_for_doc_type("statement")
    
    row = {
        "doc_id": "abc1234567890def",
        "source_url": "https://test.com",
        "fetch_timestamp": datetime.utcnow(),
        "raw_path": "test.html",
        "content_type": "text/html",
        "doc_type": "invalid_type",
    }
    
    with pytest.raises(ValueError, match="Invalid doc_type"):
        validate_row(row, schema)


def test_validate_row_invalid_doc_id():
    """Test validation fails on invalid doc_id."""
    schema = get_schema_for_doc_type("statement")
    
    # Too short
    row = {
        "doc_id": "abc123",
        "source_url": "https://test.com",
        "fetch_timestamp": datetime.utcnow(),
        "raw_path": "test.html",
        "content_type": "text/html",
        "doc_type": "statement",
    }
    
    with pytest.raises(ValueError, match="doc_id must be 16-character string"):
        validate_row(row, schema)


def test_validate_rows():
    """Test validating multiple rows."""
    rows = [
        {
            "doc_id": "abc1234567890def",
            "source_url": "https://test1.com",
            "fetch_timestamp": datetime.utcnow(),
            "raw_path": "test1.html",
            "content_type": "text/html",
            "doc_type": "statement",
        },
        {
            "doc_id": "def1234567890abc",
            "source_url": "https://test2.com",
            "fetch_timestamp": datetime.utcnow(),
            "raw_path": "test2.html",
            "content_type": "text/html",
            "doc_type": "statement",
        },
    ]
    
    # Should not raise
    validate_rows(rows, "statement")


def test_valid_doc_types():
    """Test that valid doc types are defined."""
    assert "statement" in VALID_DOC_TYPES
    assert "minutes" in VALID_DOC_TYPES
    assert "speech" in VALID_DOC_TYPES
    assert "press_conference" in VALID_DOC_TYPES
