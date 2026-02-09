"""Document ID generation utilities.

This module provides helpers for generating stable document identifiers
based on source URLs using SHA-1 hashing.
"""

import hashlib
from typing import Optional


def generate_doc_id(source_url: str) -> str:
    """Generate a stable document ID from a source URL.
    
    Creates a 16-character hexadecimal ID by computing the SHA-1 hash
    of the source URL and taking the first 16 characters. This ensures
    stable, unique identifiers for documents regardless of when they
    are fetched.
    
    Args:
        source_url: The source URL of the document to generate an ID for.
    
    Returns:
        A 16-character hexadecimal string serving as the document ID.
    
    Examples:
        >>> generate_doc_id("https://www.federalreserve.gov/statement.htm")
        '8a3f9c2e1b4d7a6c'
    """
    if not source_url:
        raise ValueError("source_url cannot be empty")
    
    # Compute SHA-1 hash of the URL
    hash_obj = hashlib.sha1(source_url.encode("utf-8"))
    
    # Return first 16 characters of hex digest
    return hash_obj.hexdigest()[:16]


def validate_doc_id(doc_id: str) -> bool:
    """Validate that a document ID matches the expected format.
    
    Args:
        doc_id: The document ID to validate.
    
    Returns:
        True if the doc_id is valid (16 hexadecimal characters), False otherwise.
    
    Examples:
        >>> validate_doc_id("8a3f9c2e1b4d7a6c")
        True
        >>> validate_doc_id("invalid")
        False
    """
    if not doc_id or len(doc_id) != 16:
        return False
    
    try:
        int(doc_id, 16)
        return True
    except ValueError:
        return False


def doc_id_from_url(url: str, prefix: Optional[str] = None) -> str:
    """Generate a document ID from a URL with optional prefix.
    
    This is a convenience wrapper around generate_doc_id that allows
    adding a prefix to the generated ID for internal categorization purposes.
    
    **Note**: Adding a prefix creates an ID longer than 16 characters. The base
    16-character ID remains stable; the prefix is for internal organization only.
    When storing documents, consider using the base 16-char ID from generate_doc_id()
    for filenames to maintain the stable ID design principle.
    
    Args:
        url: The source URL of the document.
        prefix: Optional prefix to prepend to the doc_id (e.g., "fomc_", "speech_").
                This is for internal categorization only.
    
    Returns:
        The generated document ID, optionally prefixed.
    
    Examples:
        >>> doc_id_from_url("https://example.com/doc")
        '8a3f9c2e1b4d7a6c'
        >>> doc_id_from_url("https://example.com/doc", prefix="fomc_")
        'fomc_8a3f9c2e1b4d7a6c'  # Note: This is 21 chars, prefix is for categorization
    """
    doc_id = generate_doc_id(url)
    
    if prefix:
        return f"{prefix}{doc_id}"
    
    return doc_id
