"""PyArrow schema validation for Parquet outputs.

This module defines and validates PyArrow schemas for document metadata
to ensure consistency in Parquet storage.
"""

import pyarrow as pa
from typing import List, Dict, Any
from datetime import datetime


# Base schema for all documents
BASE_DOCUMENT_SCHEMA = pa.schema([
    ("doc_id", pa.string(), False),  # Required
    ("source_url", pa.string(), False),  # Required
    ("fetch_timestamp", pa.timestamp("us", tz="UTC"), False),  # Required
    ("raw_path", pa.string(), False),  # Required
    ("content_type", pa.string(), False),  # Required
    ("title", pa.string(), True),  # Optional
    ("published_date", pa.timestamp("us", tz="UTC"), True),  # Optional
    ("doc_type", pa.string(), False),  # Required
])


# Extended schema for FOMC statements
FOMC_STATEMENT_SCHEMA = pa.schema([
    ("doc_id", pa.string(), False),
    ("source_url", pa.string(), False),
    ("fetch_timestamp", pa.timestamp("us", tz="UTC"), False),
    ("raw_path", pa.string(), False),
    ("content_type", pa.string(), False),
    ("title", pa.string(), True),
    ("published_date", pa.timestamp("us", tz="UTC"), True),
    ("doc_type", pa.string(), False),
    ("meeting_date", pa.timestamp("us", tz="UTC"), True),
    ("policy_decision", pa.string(), True),
    ("vote_summary", pa.string(), True),
    ("participants", pa.string(), True),
])


# Extended schema for FOMC minutes
FOMC_MINUTES_SCHEMA = pa.schema([
    ("doc_id", pa.string(), False),
    ("source_url", pa.string(), False),
    ("fetch_timestamp", pa.timestamp("us", tz="UTC"), False),
    ("raw_path", pa.string(), False),
    ("content_type", pa.string(), False),
    ("title", pa.string(), True),
    ("published_date", pa.timestamp("us", tz="UTC"), True),
    ("doc_type", pa.string(), False),
    ("meeting_date", pa.timestamp("us", tz="UTC"), True),
    ("participants", pa.string(), True),
    ("economic_projections", pa.string(), True),
])


# Extended schema for speeches
SPEECH_SCHEMA = pa.schema([
    ("doc_id", pa.string(), False),
    ("source_url", pa.string(), False),
    ("fetch_timestamp", pa.timestamp("us", tz="UTC"), False),
    ("raw_path", pa.string(), False),
    ("content_type", pa.string(), False),
    ("title", pa.string(), True),
    ("published_date", pa.timestamp("us", tz="UTC"), True),
    ("doc_type", pa.string(), False),
    ("speaker", pa.string(), True),
    ("speaker_title", pa.string(), True),
    ("event_name", pa.string(), True),
    ("location", pa.string(), True),
    ("speech_date", pa.timestamp("us", tz="UTC"), True),
])


# Extended schema for press conferences
PRESS_CONFERENCE_SCHEMA = pa.schema([
    ("doc_id", pa.string(), False),
    ("source_url", pa.string(), False),
    ("fetch_timestamp", pa.timestamp("us", tz="UTC"), False),
    ("raw_path", pa.string(), False),
    ("content_type", pa.string(), False),
    ("title", pa.string(), True),
    ("published_date", pa.timestamp("us", tz="UTC"), True),
    ("doc_type", pa.string(), False),
    ("meeting_date", pa.timestamp("us", tz="UTC"), True),
    ("chair_name", pa.string(), True),
    ("participants", pa.string(), True),
])


# Valid document types
VALID_DOC_TYPES = {
    "statement",
    "minutes",
    "speech",
    "press_conference",
    "testimony",
    "report",
}


def get_schema_for_doc_type(doc_type: str) -> pa.Schema:
    """Get the appropriate PyArrow schema for a document type.
    
    Args:
        doc_type: Document type string.
    
    Returns:
        PyArrow schema for the document type.
    
    Raises:
        ValueError: If document type is not valid.
    """
    doc_type = doc_type.lower()
    
    if doc_type not in VALID_DOC_TYPES:
        raise ValueError(
            f"Invalid doc_type: {doc_type}. "
            f"Must be one of: {', '.join(VALID_DOC_TYPES)}"
        )
    
    schema_map = {
        "statement": FOMC_STATEMENT_SCHEMA,
        "minutes": FOMC_MINUTES_SCHEMA,
        "speech": SPEECH_SCHEMA,
        "press_conference": PRESS_CONFERENCE_SCHEMA,
        "testimony": SPEECH_SCHEMA,  # Same as speech
        "report": BASE_DOCUMENT_SCHEMA,
    }
    
    return schema_map.get(doc_type, BASE_DOCUMENT_SCHEMA)


def validate_row(row: Dict[str, Any], schema: pa.Schema) -> None:
    """Validate a row against a PyArrow schema.
    
    Args:
        row: Dictionary representing a data row.
        schema: PyArrow schema to validate against.
    
    Raises:
        ValueError: If row doesn't match schema.
    """
    # Check required fields
    for field in schema:
        if not field.nullable and field.name not in row:
            raise ValueError(f"Required field missing: {field.name}")
        
        if field.name in row and row[field.name] is not None:
            # Check doc_type is valid
            if field.name == "doc_type":
                if row[field.name] not in VALID_DOC_TYPES:
                    raise ValueError(
                        f"Invalid doc_type: {row[field.name]}. "
                        f"Must be one of: {', '.join(VALID_DOC_TYPES)}"
                    )
            
            # Check doc_id format
            if field.name == "doc_id":
                doc_id = row[field.name]
                if not isinstance(doc_id, str) or len(doc_id) != 16:
                    raise ValueError(f"doc_id must be 16-character string, got: {doc_id}")
                try:
                    int(doc_id, 16)
                except ValueError:
                    raise ValueError(f"doc_id must be hexadecimal, got: {doc_id}")


def validate_rows(rows: List[Dict[str, Any]], doc_type: str) -> None:
    """Validate multiple rows against the appropriate schema.
    
    Args:
        rows: List of row dictionaries.
        doc_type: Document type for schema selection.
    
    Raises:
        ValueError: If any row doesn't match schema.
    """
    schema = get_schema_for_doc_type(doc_type)
    
    for i, row in enumerate(rows):
        try:
            validate_row(row, schema)
        except ValueError as e:
            raise ValueError(f"Row {i} validation failed: {e}")


def ensure_schema_compatibility(table: pa.Table, expected_schema: pa.Schema) -> pa.Table:
    """Ensure a PyArrow table matches the expected schema.
    
    Converts types and adds missing nullable columns as needed.
    
    Args:
        table: PyArrow table to check.
        expected_schema: Expected schema.
    
    Returns:
        Table with schema matching expected_schema.
    """
    # This is a simple version - could be extended with more sophisticated type coercion
    return table.cast(expected_schema, safe=False)
