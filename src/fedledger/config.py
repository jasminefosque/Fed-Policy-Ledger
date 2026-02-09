"""Configuration management for Fed Policy Ledger.

This module provides a centralized configuration system using pydantic-settings
for managing application settings with environment variable support.
"""

from pathlib import Path
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class FedLedgerConfig(BaseSettings):
    """Central configuration for Fed Policy Ledger.
    
    Configuration values can be set via:
    - Environment variables (prefixed with FEDLEDGER_)
    - .env file
    - Direct instantiation
    
    Attributes:
        data_dir: Base directory for data storage.
        raw_dir: Directory for raw HTML/PDF files.
        processed_dir: Directory for processed Parquet files.
        metadata_dir: Directory for JSON metadata files.
        save_raw: Whether to save raw HTML/PDF files.
        overwrite: Whether to overwrite existing files.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        log_json: Whether to output structured JSON logs.
        parallel: Enable parallel processing.
        max_workers: Maximum number of parallel workers.
        cache_dir: Directory for HTTP cache.
        user_agent: User-Agent string for HTTP requests.
    """
    
    # Directory settings
    data_dir: Path = Field(
        default=Path("data"),
        description="Base directory for data storage"
    )
    raw_dir: Optional[Path] = Field(
        default=None,
        description="Directory for raw files (defaults to data_dir/raw)"
    )
    processed_dir: Optional[Path] = Field(
        default=None,
        description="Directory for processed files (defaults to data_dir/processed)"
    )
    metadata_dir: Optional[Path] = Field(
        default=None,
        description="Directory for metadata files (defaults to data_dir/metadata)"
    )
    
    # Processing settings
    save_raw: bool = Field(
        default=True,
        description="Save raw HTML/PDF files before processing"
    )
    overwrite: bool = Field(
        default=False,
        description="Overwrite existing files"
    )
    
    # Logging settings
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    log_json: bool = Field(
        default=False,
        description="Output structured JSON logs"
    )
    
    # Parallelization settings
    parallel: bool = Field(
        default=False,
        description="Enable parallel processing"
    )
    max_workers: int = Field(
        default=4,
        description="Maximum number of parallel workers"
    )
    
    # HTTP settings
    cache_dir: Optional[Path] = Field(
        default=None,
        description="Directory for HTTP cache"
    )
    user_agent: str = Field(
        default="FedPolicyLedger/0.1.0 (Research/Archival)",
        description="User-Agent string for HTTP requests"
    )
    
    model_config = SettingsConfigDict(
        env_prefix="FEDLEDGER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    def __init__(self, **kwargs):
        """Initialize configuration and set default paths."""
        super().__init__(**kwargs)
        
        # Set default subdirectories if not specified
        if self.raw_dir is None:
            self.raw_dir = self.data_dir / "raw"
        if self.processed_dir is None:
            self.processed_dir = self.data_dir / "processed"
        if self.metadata_dir is None:
            self.metadata_dir = self.data_dir / "metadata"
    
    def ensure_directories(self):
        """Create all configured directories if they don't exist."""
        for dir_path in [
            self.data_dir,
            self.raw_dir,
            self.processed_dir,
            self.metadata_dir,
        ]:
            if dir_path:
                dir_path.mkdir(parents=True, exist_ok=True)
        
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
