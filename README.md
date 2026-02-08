# Fed Policy Ledger

Fed Policy Ledger is a structured archive of Federal Reserve policy communications. It records FOMC statements, minutes, press conference transcripts, speeches, and member activity as immutable source files with normalized metadata for policy tracing, narrative analysis, and market impact research.

## Overview

This repository preserves Federal Reserve communications by:
- Saving raw HTML and PDFs to `data/raw/` before parsing
- Using stable document IDs: `doc_id = sha1(source_url)[:16]`
- Keeping extraction separate from transformation
- Never inventing endpoints or fields

## Project Structure

```
Fed-Policy-Ledger/
├── src/fedledger/           # Core package
│   ├── __init__.py          # Package initialization
│   ├── cli.py               # Command-line interface (sync skeleton)
│   ├── http.py              # HTTP session, retries, disk cache
│   ├── ids.py               # Document ID helpers
│   └── models.py            # Shared data models
├── data/
│   └── raw/                 # Raw HTML and PDF documents
├── pyproject.toml           # Project configuration
├── .gitignore              # Git ignore rules
├── LICENSE                  # MIT License
└── README.md               # This file
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

The CLI is currently a skeleton with planned commands:

```bash
# Sync documents (not yet implemented)
fedledger sync                    # Sync all document types
fedledger sync --type statements  # Sync only FOMC statements

# List archived documents (not yet implemented)
fedledger list

# Show document details (not yet implemented)
fedledger info <doc_id>

# Show archive statistics (not yet implemented)
fedledger stats
```

## Core Modules

### `ids.py` - Document ID Generation

Generates stable 16-character hexadecimal IDs from source URLs:

```python
from fedledger.ids import generate_doc_id

doc_id = generate_doc_id("https://www.federalreserve.gov/statement.htm")
# Returns: "dc11288aa80a47e9"
```

### `models.py` - Data Models

Defines data structures for documents:

```python
from fedledger.models import Document, FOMCStatement, Speech
from datetime import datetime

doc = Document(
    doc_id="dc11288aa80a47e9",
    source_url="https://www.federalreserve.gov/statement.htm",
    fetch_timestamp=datetime.now(),
    raw_path="data/raw/dc11288aa80a47e9.html",
    content_type="text/html"
)
```

### `http.py` - HTTP Client

Provides session management with retries and disk caching:

```python
from fedledger.http import HTTPSession
from pathlib import Path

with HTTPSession(cache_dir=Path(".cache")) as session:
    response = session.get("https://www.federalreserve.gov/")
```

### `cli.py` - Command-Line Interface

Entry point for the command-line tool with subcommands for sync, list, info, and stats.

## Development Status

**Current Stage:** Scaffold and core plumbing complete

- ✅ Project structure
- ✅ Core modules with docstrings
- ✅ Document ID generation
- ✅ Data models
- ✅ HTTP client skeleton
- ✅ CLI skeleton
- ⏳ Actual fetching logic (to be implemented)
- ⏳ Document parsing (to be implemented)
- ⏳ Metadata extraction (to be implemented)

## Design Principles

1. **Preservation First**: Always save raw documents before any processing
2. **Stable IDs**: Use `sha1(source_url)[:16]` for consistent identification
3. **Separation of Concerns**: Keep extraction separate from transformation
4. **No Invention**: Don't create endpoints or fields not in source data
5. **Immutability**: Raw files are never modified once saved

## License

MIT License - See LICENSE file for details
