"""End-to-end integration tests for the pipeline."""

import pytest
from pathlib import Path
import json
import pyarrow.parquet as pq

from fedledger.config import FedLedgerConfig
from fedledger.pipeline import Pipeline
from fedledger.pydantic_models import DocumentType
from fedledger.ids import generate_doc_id


@pytest.fixture
def fixtures_dir():
    """Get fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def temp_config(tmp_path):
    """Create temporary configuration."""
    config = FedLedgerConfig(
        data_dir=tmp_path / "data",
        save_raw=True,
        parallel=False,
    )
    config.ensure_directories()
    return config


def test_pipeline_discovery(fixtures_dir, temp_config):
    """Test document discovery."""
    pipeline = Pipeline(temp_config)
    
    files = pipeline.discover_local_files(fixtures_dir, "*.html")
    
    assert len(files) >= 3
    assert all(f.suffix == ".html" for f in files)


def test_pipeline_process_single_document(fixtures_dir, temp_config):
    """Test processing a single document."""
    pipeline = Pipeline(temp_config)
    
    statement_file = fixtures_dir / "statement_20240131.html"
    assert statement_file.exists()
    
    doc = pipeline.process_document(
        source_path=statement_file,
        doc_type=DocumentType.STATEMENT,
        source_url="https://www.federalreserve.gov/statement.htm",
    )
    
    assert doc is not None
    assert doc.doc_id == generate_doc_id("https://www.federalreserve.gov/statement.htm")
    assert doc.doc_type == DocumentType.STATEMENT
    assert doc.raw_path is not None


def test_pipeline_process_parallel(fixtures_dir, temp_config):
    """Test parallel document processing."""
    temp_config.parallel = True
    temp_config.max_workers = 2
    
    pipeline = Pipeline(temp_config)
    
    files = pipeline.discover_local_files(fixtures_dir, "*.html")
    documents = pipeline.process_documents_parallel(files, DocumentType.STATEMENT)
    
    assert len(documents) >= 3
    assert all(doc.doc_id for doc in documents)


def test_pipeline_write_parquet(fixtures_dir, temp_config):
    """Test writing documents to Parquet."""
    pipeline = Pipeline(temp_config)
    
    files = pipeline.discover_local_files(fixtures_dir, "*.html")
    documents = pipeline.process_documents_parallel(files[:1], DocumentType.STATEMENT)
    
    output_path = temp_config.processed_dir / "test_documents.parquet"
    pipeline.write_parquet(documents, output_path, DocumentType.STATEMENT)
    
    assert output_path.exists()
    
    # Verify Parquet file can be read
    table = pq.read_table(output_path)
    assert len(table) == len(documents)
    assert "doc_id" in table.column_names
    assert "source_url" in table.column_names


def test_pipeline_write_json_metadata(fixtures_dir, temp_config):
    """Test writing JSON metadata."""
    pipeline = Pipeline(temp_config)
    
    files = pipeline.discover_local_files(fixtures_dir, "*.html")
    documents = pipeline.process_documents_parallel(files[:1], DocumentType.STATEMENT)
    
    output_path = temp_config.metadata_dir / "test_metadata.json"
    pipeline.write_json_metadata(documents, output_path)
    
    assert output_path.exists()
    
    # Verify JSON can be read
    with open(output_path, "r") as f:
        metadata = json.load(f)
    
    assert len(metadata) == len(documents)
    assert metadata[0]["doc_id"]


def test_pipeline_full_run(fixtures_dir, temp_config):
    """Test complete pipeline execution."""
    pipeline = Pipeline(temp_config)
    
    result = pipeline.run(
        source_directory=fixtures_dir,
        doc_type=DocumentType.STATEMENT,
        pattern="*.html",
        limit=2,
    )
    
    assert result.success
    assert result.documents_processed >= 1
    assert len(result.output_files) == 2  # Parquet + JSON
    
    # Verify output files exist
    for output_file in result.output_files:
        assert output_file.exists()


def test_pipeline_deterministic_doc_ids(fixtures_dir, temp_config):
    """Test that doc_id generation is stable and deterministic."""
    pipeline = Pipeline(temp_config)
    
    statement_file = fixtures_dir / "statement_20240131.html"
    source_url = "https://www.federalreserve.gov/statement.htm"
    
    # Process same document twice
    doc1 = pipeline.process_document(statement_file, DocumentType.STATEMENT, source_url)
    doc2 = pipeline.process_document(statement_file, DocumentType.STATEMENT, source_url)
    
    # Doc IDs should be identical
    assert doc1.doc_id == doc2.doc_id
    assert len(doc1.doc_id) == 16


def test_pipeline_schema_validation(fixtures_dir, temp_config):
    """Test that output schema is validated correctly."""
    pipeline = Pipeline(temp_config)
    
    result = pipeline.run(
        source_directory=fixtures_dir,
        doc_type=DocumentType.STATEMENT,
        pattern="statement_*.html",
        limit=1,
    )
    
    assert result.success
    
    # Read Parquet and verify schema
    parquet_file = temp_config.processed_dir / "statement_documents.parquet"
    table = pq.read_table(parquet_file)
    
    # Check required fields
    required_fields = ["doc_id", "source_url", "fetch_timestamp", "raw_path", "content_type", "doc_type"]
    for field in required_fields:
        assert field in table.column_names
    
    # Check doc_id format
    df = table.to_pandas()
    for doc_id in df["doc_id"]:
        assert len(doc_id) == 16
        int(doc_id, 16)  # Should be valid hex
