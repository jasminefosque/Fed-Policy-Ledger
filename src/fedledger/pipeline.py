"""Pipeline orchestrator for Fed Policy Ledger.

This module provides a modular pipeline for discovering, downloading,
parsing, and storing Federal Reserve documents.
"""

import asyncio
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from fedledger.config import FedLedgerConfig
from fedledger.logging_config import get_logger
from fedledger.pydantic_models import (
    BaseDocument,
    FOMCStatementModel,
    SpeechModel,
    DocumentModel,
    DocumentType,
)
from fedledger.schema import get_schema_for_doc_type, validate_rows
from fedledger.ids import generate_doc_id


logger = get_logger(__name__)


class PipelineResult:
    """Result of a pipeline execution.
    
    Attributes:
        success: Whether pipeline completed successfully.
        documents_processed: Number of documents processed.
        documents_failed: Number of documents that failed.
        output_files: List of output file paths.
        errors: List of error messages.
    """
    
    def __init__(self):
        self.success = True
        self.documents_processed = 0
        self.documents_failed = 0
        self.output_files: List[Path] = []
        self.errors: List[str] = []
    
    def add_success(self):
        """Record a successful document."""
        self.documents_processed += 1
    
    def add_failure(self, error: str):
        """Record a failed document."""
        self.documents_failed += 1
        self.errors.append(error)
        if self.documents_failed > 0:
            self.success = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "documents_processed": self.documents_processed,
            "documents_failed": self.documents_failed,
            "output_files": [str(p) for p in self.output_files],
            "errors": self.errors,
        }


class Pipeline:
    """Orchestrates document processing pipeline.
    
    The pipeline coordinates:
    1. Document discovery (from directory, URLs, or RSS feeds)
    2. Downloading or loading raw files
    3. Parsing and extraction
    4. Metadata normalization using Pydantic models
    5. Writing outputs (Parquet, JSON metadata)
    6. Optional raw file preservation
    
    The pipeline is modular and allows plugging in custom extractors.
    """
    
    def __init__(self, config: Optional[FedLedgerConfig] = None):
        """Initialize pipeline.
        
        Args:
            config: Configuration object. Creates default if None.
        """
        self.config = config or FedLedgerConfig()
        self.config.ensure_directories()
        
        # Registry of extractors by document type
        self.extractors: Dict[str, Callable] = {}
    
    def register_extractor(
        self,
        doc_type: DocumentType,
        extractor: Callable[[str, str, str, str], DocumentModel]
    ):
        """Register an extractor function for a document type.
        
        Args:
            doc_type: Document type to register extractor for.
            extractor: Function that takes (html_content, doc_id, source_url, raw_path)
                      and returns a DocumentModel instance.
        """
        self.extractors[doc_type.value] = extractor
        logger.info(f"Registered extractor for {doc_type.value}")
    
    def discover_local_files(
        self,
        directory: Path,
        pattern: str = "*.html"
    ) -> List[Path]:
        """Discover files in a local directory.
        
        Args:
            directory: Directory to search.
            pattern: Glob pattern for files.
        
        Returns:
            List of file paths.
        """
        directory = Path(directory)
        if not directory.exists():
            logger.warning(f"Directory does not exist: {directory}")
            return []
        
        files = list(directory.glob(pattern))
        logger.info(f"Discovered {len(files)} files in {directory}")
        return files
    
    def process_document(
        self,
        source_path: Path,
        doc_type: DocumentType,
        source_url: Optional[str] = None,
    ) -> Optional[DocumentModel]:
        """Process a single document.
        
        Args:
            source_path: Path to source file.
            doc_type: Type of document.
            source_url: Optional source URL. If None, uses file path.
        
        Returns:
            Processed DocumentModel or None if failed.
        """
        try:
            # Generate doc_id
            url = source_url or f"file://{source_path}"
            doc_id = generate_doc_id(url)
            
            doc_logger = get_logger(__name__, doc_id=doc_id, doc_type=doc_type.value)
            doc_logger.info(f"Processing document from {source_path}")
            
            # Read raw content
            with open(source_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            
            # Determine raw path
            if self.config.save_raw:
                raw_filename = f"{doc_id}{source_path.suffix}"
                raw_path = self.config.raw_dir / raw_filename
                
                # Save raw file if not already there
                if not raw_path.exists() or self.config.overwrite:
                    raw_path.write_text(content, encoding="utf-8")
                    doc_logger.debug(f"Saved raw file to {raw_path}")
            else:
                raw_path = source_path
            
            # Get extractor
            extractor = self.extractors.get(doc_type.value)
            
            if extractor:
                # Use custom extractor
                document = extractor(content, doc_id, url, str(raw_path))
            else:
                # Use default: create base model
                doc_logger.warning(f"No extractor registered for {doc_type.value}, using base model")
                document = BaseDocument(
                    doc_id=doc_id,
                    source_url=url,
                    fetch_timestamp=datetime.utcnow(),
                    raw_path=str(raw_path),
                    content_type="text/html" if source_path.suffix == ".html" else "application/pdf",
                    doc_type=doc_type,
                )
            
            doc_logger.info(f"Successfully processed document {doc_id}")
            return document
            
        except Exception as e:
            logger.error(f"Failed to process {source_path}: {e}", exc_info=True)
            return None
    
    def process_documents_parallel(
        self,
        source_paths: List[Path],
        doc_type: DocumentType,
    ) -> List[DocumentModel]:
        """Process multiple documents in parallel.
        
        Args:
            source_paths: List of source file paths.
            doc_type: Type of documents.
        
        Returns:
            List of processed documents.
        """
        documents = []
        
        if not self.config.parallel:
            # Sequential processing
            for path in source_paths:
                doc = self.process_document(path, doc_type)
                if doc:
                    documents.append(doc)
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                futures = {
                    executor.submit(self.process_document, path, doc_type): path
                    for path in source_paths
                }
                
                for future in as_completed(futures):
                    try:
                        doc = future.result()
                        if doc:
                            documents.append(doc)
                    except Exception as e:
                        path = futures[future]
                        logger.error(f"Failed to process {path}: {e}")
        
        return documents
    
    def write_parquet(
        self,
        documents: List[DocumentModel],
        output_path: Path,
        doc_type: DocumentType,
    ) -> None:
        """Write documents to Parquet file.
        
        Args:
            documents: List of document models.
            output_path: Output Parquet file path.
            doc_type: Document type for schema validation.
        """
        if not documents:
            logger.warning("No documents to write")
            return
        
        # Convert to rows
        rows = [doc.to_parquet_row() for doc in documents]
        
        # Validate schema
        validate_rows(rows, doc_type.value)
        
        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Convert datetime columns
        datetime_cols = ["fetch_timestamp", "published_date", "meeting_date", "speech_date"]
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
        
        # Get schema
        schema = get_schema_for_doc_type(doc_type.value)
        
        # Write to Parquet
        table = pa.Table.from_pandas(df, schema=schema, safe=False)
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, output_path)
        
        logger.info(f"Wrote {len(documents)} documents to {output_path}")
    
    def write_json_metadata(
        self,
        documents: List[DocumentModel],
        output_path: Path,
    ) -> None:
        """Write documents metadata to JSON file.
        
        Args:
            documents: List of document models.
            output_path: Output JSON file path.
        """
        if not documents:
            logger.warning("No documents to write")
            return
        
        # Convert to JSON-serializable format
        metadata = [doc.to_json_metadata() for doc in documents]
        
        # Write to JSON
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Wrote metadata for {len(documents)} documents to {output_path}")
    
    def run(
        self,
        source_directory: Path,
        doc_type: DocumentType,
        pattern: str = "*.html",
        limit: Optional[int] = None,
    ) -> PipelineResult:
        """Run the full pipeline.
        
        Args:
            source_directory: Directory containing source files.
            doc_type: Type of documents to process.
            pattern: File pattern to match.
            limit: Optional limit on number of documents to process.
        
        Returns:
            PipelineResult with execution details.
        """
        result = PipelineResult()
        
        try:
            logger.info(f"Starting pipeline for {doc_type.value} documents")
            
            # 1. Discover documents
            files = self.discover_local_files(source_directory, pattern)
            
            if limit:
                files = files[:limit]
                logger.info(f"Limited to {limit} files")
            
            if not files:
                logger.warning("No files found to process")
                return result
            
            # 2. Process documents
            documents = self.process_documents_parallel(files, doc_type)
            
            result.documents_processed = len(documents)
            result.documents_failed = len(files) - len(documents)
            
            if not documents:
                logger.error("No documents were successfully processed")
                result.success = False
                return result
            
            # 3. Write Parquet output
            parquet_path = self.config.processed_dir / f"{doc_type.value}_documents.parquet"
            self.write_parquet(documents, parquet_path, doc_type)
            result.output_files.append(parquet_path)
            
            # 4. Write JSON metadata
            json_path = self.config.metadata_dir / f"{doc_type.value}_metadata.json"
            self.write_json_metadata(documents, json_path)
            result.output_files.append(json_path)
            
            logger.info(
                f"Pipeline completed: {result.documents_processed} processed, "
                f"{result.documents_failed} failed"
            )
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            result.success = False
            result.add_failure(str(e))
        
        return result
