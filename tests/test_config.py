"""Test configuration module."""

import pytest
from pathlib import Path
from fedledger.config import FedLedgerConfig


def test_config_default_values():
    """Test configuration with default values."""
    config = FedLedgerConfig()
    
    assert config.data_dir == Path("data")
    assert config.raw_dir == Path("data/raw")
    assert config.processed_dir == Path("data/processed")
    assert config.metadata_dir == Path("data/metadata")
    assert config.save_raw is True
    assert config.overwrite is False
    assert config.log_level == "INFO"
    assert config.parallel is False
    assert config.max_workers == 4


def test_config_custom_values():
    """Test configuration with custom values."""
    config = FedLedgerConfig(
        data_dir=Path("/tmp/test_data"),
        save_raw=False,
        parallel=True,
        max_workers=8,
        log_level="DEBUG",
    )
    
    assert config.data_dir == Path("/tmp/test_data")
    assert config.raw_dir == Path("/tmp/test_data/raw")
    assert config.save_raw is False
    assert config.parallel is True
    assert config.max_workers == 8
    assert config.log_level == "DEBUG"


def test_config_ensure_directories(tmp_path):
    """Test directory creation."""
    config = FedLedgerConfig(data_dir=tmp_path / "test")
    config.ensure_directories()
    
    assert config.data_dir.exists()
    assert config.raw_dir.exists()
    assert config.processed_dir.exists()
    assert config.metadata_dir.exists()
