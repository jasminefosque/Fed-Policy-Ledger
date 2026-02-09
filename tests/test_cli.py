"""Test CLI commands."""

import pytest
from pathlib import Path
import sys
from io import StringIO

from fedledger.cli import main


@pytest.fixture
def fixtures_dir():
    """Get fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create temporary data directory."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


def test_cli_help():
    """Test CLI help command."""
    result = main([])
    assert result == 0


def test_cli_sync_dry_run(fixtures_dir, temp_data_dir, capsys):
    """Test sync command with dry-run."""
    result = main([
        "--data-dir", str(temp_data_dir),
        "sync",
        str(fixtures_dir),
        "--type", "statements",
        "--dry-run",
    ])
    
    assert result == 0
    
    # Verify no files were created
    processed_dir = temp_data_dir / "processed"
    if processed_dir.exists():
        parquet_files = list(processed_dir.glob("*.parquet"))
        assert len(parquet_files) == 0


def test_cli_sync_basic(fixtures_dir, temp_data_dir):
    """Test basic sync command."""
    result = main([
        "--data-dir", str(temp_data_dir),
        "sync",
        str(fixtures_dir),
        "--type", "statements",
        "--save-raw",
        "--limit", "2",
    ])
    
    assert result == 0
    
    # Verify output files were created
    processed_dir = temp_data_dir / "processed"
    metadata_dir = temp_data_dir / "metadata"
    
    assert processed_dir.exists()
    assert metadata_dir.exists()
    
    parquet_files = list(processed_dir.glob("*.parquet"))
    assert len(parquet_files) >= 1
    
    json_files = list(metadata_dir.glob("*.json"))
    assert len(json_files) >= 1


def test_cli_sync_parallel(fixtures_dir, temp_data_dir):
    """Test sync with parallel processing."""
    result = main([
        "--data-dir", str(temp_data_dir),
        "sync",
        str(fixtures_dir),
        "--type", "statements",
        "--parallel",
        "--workers", "2",
        "--limit", "3",
    ])
    
    assert result == 0


def test_cli_list(temp_data_dir, fixtures_dir):
    """Test list command after sync."""
    # First sync some documents
    main([
        "--data-dir", str(temp_data_dir),
        "sync",
        str(fixtures_dir),
        "--type", "statements",
        "--limit", "1",
    ])
    
    # Then list them
    result = main([
        "--data-dir", str(temp_data_dir),
        "list",
        "--format", "table",
    ])
    
    assert result == 0


def test_cli_stats(temp_data_dir, fixtures_dir):
    """Test stats command after sync."""
    # First sync some documents
    main([
        "--data-dir", str(temp_data_dir),
        "sync",
        str(fixtures_dir),
        "--type", "statements",
        "--limit", "2",
    ])
    
    # Then get stats
    result = main([
        "--data-dir", str(temp_data_dir),
        "stats",
    ])
    
    assert result == 0


def test_cli_verbose_mode(fixtures_dir, temp_data_dir):
    """Test CLI with verbose flag."""
    result = main([
        "--data-dir", str(temp_data_dir),
        "--verbose",
        "sync",
        str(fixtures_dir),
        "--type", "statements",
        "--limit", "1",
    ])
    
    assert result == 0


def test_cli_invalid_command():
    """Test CLI with invalid command."""
    # argparse raises SystemExit, so we need to catch it
    with pytest.raises(SystemExit):
        main(["invalid_command"])
