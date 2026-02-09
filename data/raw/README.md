# Data Directory Structure

This directory contains the Fed Policy Ledger data organized by processing stage.

## Directory Layout

```
data/
├── raw/          # Raw, unmodified documents (HTML, PDF) as fetched
├── processed/    # Extracted and normalized data (future)
└── metadata/     # Document metadata and indexes (future)
```

## Raw Data (`raw/`)

The `raw/` directory preserves Federal Reserve communications in their original format:

- **Purpose**: Immutable archive of source documents
- **Format**: Original HTML files, PDFs, and other formats as fetched
- **Naming**: Files are named using stable document IDs: `{doc_id}.{extension}`
- **Policy**: Never modify or delete files once saved

### Example Structure

```
raw/
├── 8a3f9c2e1b4d7a6c.html     # FOMC statement (HTML)
├── 1f2e3d4c5b6a7980.pdf      # Federal Reserve speech (PDF)
└── 9c8b7a6f5e4d3c2b.html     # Press conference transcript
```

## Document ID Scheme

All documents use a stable 16-character hexadecimal ID:
- Generated as: `sha1(source_url)[:16]`
- Ensures consistent identification across fetches
- Collision-resistant for the expected document volume

## Notes

- Keep extraction separate from transformation
- Raw files are the source of truth
- Do not invent endpoints or modify source data
