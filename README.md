# Fed Policy Ledger

Fed Policy Ledger is a production-ready data pipeline for archiving Federal Reserve policy communications. It preserves FOMC statements, minutes, press conference transcripts, speeches, and member activity as immutable source files with normalized metadata for policy tracing, narrative analysis, and market impact research.

## Overview

This repository provides enterprise-grade tooling for:
- **Raw Preservation**: Saving raw HTML and PDFs to `data/raw/` before parsing
- **Stable IDs**: Using `doc_id = sha1(source_url)[:16]` for consistent identification
- **Type Safety**: Pydantic models with validation and schema enforcement
- **Parallel Processing**: ThreadPoolExecutor for batch document processing
- **Structured Logging**: JSON logs with context (doc_id, timestamps, errors)
- **Rich CLI**: Progress bars, colored output, tables, and panels
- **Schema Validation**: PyArrow schemas for Parquet output consistency

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         Fed Policy Ledger                        │
│                       Production Pipeline                        │
└──────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│   CLI (rich)    │  ← User interaction with progress bars & tables
│   cli.py        │
└────────┬────────┘
         │
         ↓
┌────────────────────────────────────────────────────────────────┐
│                    Pipeline Orchestrator                       │
│  • Discovery    • Download     • Parse      • Normalize        │
│  • Validate     • Write        • Parallel   • Error Handling   │
│                    pipeline.py                                 │
└─────┬──────────────────────────────────────────────────┬───────┘
      │                                                   │
      ↓                                                   ↓
┌─────────────────┐                           ┌──────────────────┐
│   Extractors    │                           │  Pydantic Models │
│  (pluggable)    │                           │  • BaseDocument  │
│                 │←─────────────────────────→│  • FOMCStatement │
│ • HTML Parser   │                           │  • Speech        │
│ • PDF Parser    │                           │  • Minutes       │
│ • Metadata      │                           │  • Validation    │
└─────────────────┘                           └──────────────────┘
      │                                                   │
      │                                                   │
      ↓                                                   ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Data Normalizers                           │
│  • .to_parquet_row()    • .to_json_metadata()                  │
│  • Type conversion       • Schema alignment                     │
└─────┬───────────────────────────────────────────────────┬───────┘
      │                                                   │
      ↓                                                   ↓
┌──────────────────┐                          ┌──────────────────┐
│ PyArrow Schema   │                          │   File Writers   │
│  Validation      │                          │                  │
│                  │─────────────────────────→│ • Parquet Writer │
│ • Required fields│                          │ • JSON Writer    │
│ • Type checking  │                          │ • Raw Saver      │
│ • Doc ID format  │                          │                  │
└──────────────────┘                          └────────┬─────────┘
                                                       │
                                                       ↓
                                              ┌──────────────────┐
                                              │  Output Storage  │
                                              │                  │
                                              │ • data/raw/      │
                                              │ • data/processed/│
                                              │ • data/metadata/ │
                                              └──────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    Supporting Modules                           │
│                                                                 │
│  config.py       │ Pydantic Settings (env vars, defaults)      │
│  logging_config  │ Structured JSON logs + colored console      │
│  ids.py          │ Stable doc_id generation (SHA-1)            │
│  http.py         │ HTTP session with retries & caching         │
│  schema.py       │ PyArrow schema definitions & validation     │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
Fed-Policy-Ledger/
├── src/fedledger/                  # Core package
│   ├── __init__.py                 # Package initialization
│   ├── cli.py                      # Rich-based CLI with progress bars
│   ├── config.py                   # Pydantic configuration management
│   ├── pipeline.py                 # Pipeline orchestrator
│   ├── pydantic_models.py          # Type-safe document models
│   ├── schema.py                   # PyArrow schema validation
│   ├── logging_config.py           # Structured JSON logging
│   ├── http.py                     # HTTP session, retries, disk cache
│   ├── ids.py                      # Document ID helpers
│   └── models.py                   # Legacy dataclass models
├── tests/                          # Comprehensive test suite
│   ├── fixtures/                   # HTML/PDF test fixtures
│   ├── test_config.py             # Config tests
│   ├── test_pydantic_models.py    # Model validation tests
│   ├── test_schema.py             # Schema validation tests
│   ├── test_pipeline_e2e.py       # End-to-end pipeline tests
│   └── test_cli.py                # CLI integration tests
├── data/
│   ├── raw/                       # Raw HTML and PDF documents
│   ├── processed/                 # Parquet files
│   └── metadata/                  # JSON metadata files
├── pyproject.toml                 # Project configuration
├── .gitignore                     # Git ignore rules
├── LICENSE                        # MIT License
└── README.md                      # This file
```

## Installation

```bash
# Clone the repository
git clone https://github.com/jasminefosque/Fed-Policy-Ledger.git
cd Fed-Policy-Ledger

# Install the package
pip install -e .

# Or install with development dependencies
pip install -e ".[dev]"
```

## Usage

### Quick Start

Process documents from a local directory:

```bash
# Process FOMC statements with progress bars
fedledger sync tests/fixtures --type statements --limit 10 --save-raw

# Process with parallel workers
fedledger sync tests/fixtures --type speeches --parallel --workers 4

# Dry run to see what would be processed
fedledger sync tests/fixtures --type minutes --dry-run

# List processed documents
fedledger list --format table

# Show archive statistics
fedledger stats
```

### CLI Commands

```bash
# Sync/Process documents
fedledger sync <source_dir> --type <doc_type> [options]
  --type {statements,minutes,speeches,press_conferences}
  --limit <n>          # Limit number of documents
  --dry-run           # Show what would be done
  --save-raw          # Save raw HTML/PDF files
  --parallel          # Enable parallel processing
  --workers <n>       # Number of parallel workers
  --overwrite         # Overwrite existing files

# List processed documents  
fedledger list [options]
  --type <doc_type>   # Filter by document type
  --format {table,json,csv}

# Show document details
fedledger info <doc_id>

# Show archive statistics
fedledger stats

# Global options
  --data-dir <path>   # Base data directory (default: ./data)
  --verbose, -v       # Enable verbose logging
  --log-json          # Output structured JSON logs
```

### Python API

```python
from pathlib import Path
from fedledger.config import FedLedgerConfig
from fedledger.pipeline import Pipeline
from fedledger.pydantic_models import DocumentType, FOMCStatementModel

# Configure pipeline
config = FedLedgerConfig(
    data_dir=Path("data"),
    save_raw=True,
    parallel=True,
    max_workers=4,
)

# Create pipeline
pipeline = Pipeline(config)

# Register custom extractor (optional)
def extract_statement(html, doc_id, url, raw_path):
    # Custom HTML parsing logic
    return FOMCStatementModel(
        doc_id=doc_id,
        source_url=url,
        # ... other fields
    )

pipeline.register_extractor(DocumentType.STATEMENT, extract_statement)

# Run pipeline
result = pipeline.run(
    source_directory=Path("documents"),
    doc_type=DocumentType.STATEMENT,
    limit=100,
)

print(f"Processed: {result.documents_processed}")
print(f"Failed: {result.documents_failed}")
print(f"Outputs: {result.output_files}")
```

## Core Features

### 1. Pydantic Models with Validation

Type-safe document models with automatic validation:

```python
from fedledger.pydantic_models import FOMCStatementModel, DocumentType
from datetime import datetime

# Automatic validation on creation
statement = FOMCStatementModel(
    doc_id="dc11288aa80a47e9",  # Must be 16-char hex
    source_url="https://www.federalreserve.gov/statement.htm",
    fetch_timestamp=datetime.utcnow(),
    raw_path="data/raw/dc11288aa80a47e9.html",
    content_type="text/html",
    meeting_date=datetime(2024, 1, 31),
    policy_decision="Maintain rates at 5.25-5.50%",
)

# Convert to different formats
parquet_row = statement.to_parquet_row()
json_metadata = statement.to_json_metadata()
```

### 2. PyArrow Schema Validation

Enforce strict schemas for Parquet outputs:

```python
from fedledger.schema import get_schema_for_doc_type, validate_rows

# Get schema for document type
schema = get_schema_for_doc_type("statement")

# Validate rows before writing
rows = [doc.to_parquet_row() for doc in documents]
validate_rows(rows, "statement")  # Raises ValueError if invalid
```

### 3. Configuration Management

Environment-aware configuration with Pydantic Settings:

```python
from fedledger.config import FedLedgerConfig

# From code
config = FedLedgerConfig(
    data_dir=Path("/data"),
    parallel=True,
    max_workers=8,
)

# From environment variables (FEDLEDGER_* prefix)
# FEDLEDGER_DATA_DIR=/data
# FEDLEDGER_PARALLEL=true
# FEDLEDGER_MAX_WORKERS=8
config = FedLedgerConfig()
```

### 4. Structured Logging

JSON logs with context for debugging and monitoring:

```python
from fedledger.logging_config import setup_logging, get_logger

# Setup logging
setup_logging(level="INFO", json_output=True)

# Get logger with context
logger = get_logger(__name__, doc_id="abc123", doc_type="statement")
logger.info("Processing document")
# Output: {"timestamp": "2024-01-31T12:00:00Z", "level": "INFO", 
#          "message": "Processing document", "doc_id": "abc123", ...}
```

### 5. Parallel Processing

Process documents in parallel with ThreadPoolExecutor:

```python
# Enable in config
config = FedLedgerConfig(parallel=True, max_workers=4)
pipeline = Pipeline(config)

# Or via CLI
fedledger sync documents/ --type statements --parallel --workers 4
```

## Testing

Run the comprehensive test suite:

```bash
# Run all tests
pytest tests/ -v

# Run specific test modules
pytest tests/test_pipeline_e2e.py -v
pytest tests/test_cli.py -v

# Run with coverage
pytest tests/ --cov=fedledger --cov-report=html
```

Test coverage includes:
- **Unit tests**: Config, models, schema validation
- **Integration tests**: Pipeline processing, data writing
- **End-to-end tests**: Full pipeline runs with fixtures
- **CLI tests**: Command-line interface functionality

## Development Status

**Current Stage:** Production-ready pipeline with comprehensive features

- ✅ Project structure and packaging
- ✅ Pydantic models with validation
- ✅ PyArrow schema validation
- ✅ Pipeline orchestrator
- ✅ Parallel processing support
- ✅ Structured JSON logging
- ✅ Rich CLI with progress bars
- ✅ Configuration management
- ✅ Comprehensive test suite (28 tests)
- ⏳ Custom HTML/PDF extractors (pluggable architecture in place)
- ⏳ Remote document fetching (HTTP module ready)
- ⏳ Additional document types (architecture supports easy extension)

## Design Principles

1. **Preservation First**: Always save raw documents before any processing
2. **Stable IDs**: Use `sha1(source_url)[:16]` for consistent identification  
3. **Type Safety**: Pydantic models with validation and schema enforcement
4. **Separation of Concerns**: Keep extraction separate from transformation
5. **Modularity**: Pluggable extractors and transformers
6. **Observability**: Structured logging with context
7. **Performance**: Parallel processing for batch operations
8. **Testing**: Comprehensive test coverage with local fixtures

## Contributing

Contributions are welcome! Areas for enhancement:

- Custom extractors for specific Fed document types
- Additional data formats (CSV, JSON Lines, databases)
- Advanced caching strategies
- Document deduplication
- Incremental updates
- Cloud storage backends (S3, GCS)

## License

MIT License - See LICENSE file for details
