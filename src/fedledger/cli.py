"""Command-line interface for Fed Policy Ledger.

This module provides the main CLI entry point with rich-based UI enhancements
for synchronizing Federal Reserve documents, managing the local archive, and
querying document metadata.
"""

import sys
from pathlib import Path
from typing import Optional
import argparse
import json

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich import print as rprint

from fedledger.config import FedLedgerConfig
from fedledger.logging_config import setup_logging, get_logger
from fedledger.pipeline import Pipeline
from fedledger.pydantic_models import DocumentType


console = Console()


def setup_argparser() -> argparse.ArgumentParser:
    """Set up the command-line argument parser.
    
    Returns:
        Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        prog="fedledger",
        description="Fed Policy Ledger - Archive Federal Reserve policy communications",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  fedledger sync data/fixtures --type statements --limit 10
  fedledger sync data/fixtures --type speeches --parallel --save-raw
  fedledger list --format table
  fedledger info <doc_id>
        """
    )
    
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Base directory for data storage (default: ./data)"
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    parser.add_argument(
        "--log-json",
        action="store_true",
        help="Output structured JSON logs"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Sync command
    sync_parser = subparsers.add_parser(
        "sync",
        help="Process documents from local directory or remote sources"
    )
    sync_parser.add_argument(
        "source",
        type=Path,
        help="Source directory containing documents"
    )
    sync_parser.add_argument(
        "--type",
        choices=["statements", "minutes", "speeches", "press_conferences", "all"],
        default="statements",
        help="Type of documents to process (default: statements)"
    )
    sync_parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of documents to process"
    )
    sync_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually processing"
    )
    sync_parser.add_argument(
        "--save-raw",
        action="store_true",
        help="Save raw HTML/PDF files"
    )
    sync_parser.add_argument(
        "--parallel",
        action="store_true",
        help="Enable parallel processing"
    )
    sync_parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)"
    )
    sync_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files"
    )
    
    # List command
    list_parser = subparsers.add_parser(
        "list",
        help="List processed documents"
    )
    list_parser.add_argument(
        "--type",
        help="Filter by document type"
    )
    list_parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="table",
        help="Output format (default: table)"
    )
    
    # Info command
    info_parser = subparsers.add_parser(
        "info",
        help="Show detailed information about a document"
    )
    info_parser.add_argument(
        "doc_id",
        help="Document ID to retrieve information for"
    )
    
    # Stats command
    stats_parser = subparsers.add_parser(
        "stats",
        help="Show archive statistics"
    )
    
    return parser


def cmd_sync(args: argparse.Namespace, config: FedLedgerConfig) -> int:
    """Execute the sync command.
    
    Args:
        args: Parsed command-line arguments.
        config: Configuration object.
    
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    logger = get_logger(__name__)
    
    # Map type to DocumentType enum
    doc_type_map = {
        "statements": DocumentType.STATEMENT,
        "minutes": DocumentType.MINUTES,
        "speeches": DocumentType.SPEECH,
        "press_conferences": DocumentType.PRESS_CONFERENCE,
    }
    
    doc_type = doc_type_map.get(args.type, DocumentType.STATEMENT)
    
    # Display configuration
    console.print(Panel.fit(
        f"[bold cyan]Fed Policy Ledger - Document Processing[/bold cyan]\n\n"
        f"[yellow]Source:[/yellow] {args.source}\n"
        f"[yellow]Document Type:[/yellow] {args.type}\n"
        f"[yellow]Parallel:[/yellow] {args.parallel}\n"
        f"[yellow]Save Raw:[/yellow] {config.save_raw}\n"
        f"[yellow]Output:[/yellow] {config.processed_dir}",
        title="Configuration"
    ))
    
    if args.dry_run:
        console.print("[yellow]DRY RUN MODE - No files will be modified[/yellow]\n")
        
        # Just show what would be discovered
        pipeline = Pipeline(config)
        files = pipeline.discover_local_files(args.source, "*.html")
        
        if args.limit:
            files = files[:args.limit]
        
        table = Table(title="Documents to Process", show_header=True)
        table.add_column("File", style="cyan")
        table.add_column("Size", justify="right")
        
        for file in files:
            size = file.stat().st_size
            table.add_row(str(file.name), f"{size:,} bytes")
        
        console.print(table)
        console.print(f"\n[green]Would process {len(files)} documents[/green]")
        return 0
    
    # Create pipeline
    pipeline = Pipeline(config)
    
    # Run pipeline with progress bar
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            
            task = progress.add_task(
                f"[cyan]Processing {args.type}...",
                total=None
            )
            
            result = pipeline.run(
                source_directory=args.source,
                doc_type=doc_type,
                pattern="*.html",
                limit=args.limit,
            )
            
            progress.update(task, completed=True)
        
        # Display results
        if result.success:
            console.print(f"\n[green]✓[/green] Successfully processed {result.documents_processed} documents")
        else:
            console.print(f"\n[red]✗[/red] Failed to process some documents")
        
        if result.documents_failed > 0:
            console.print(f"[yellow]⚠[/yellow] {result.documents_failed} documents failed")
        
        # Show output files
        if result.output_files:
            table = Table(title="Output Files", show_header=True)
            table.add_column("File", style="cyan")
            table.add_column("Path", style="dim")
            
            for file_path in result.output_files:
                table.add_row(file_path.name, str(file_path))
            
            console.print("\n")
            console.print(table)
        
        return 0 if result.success else 1
        
    except Exception as e:
        logger.error(f"Sync failed: {e}", exc_info=True)
        console.print(f"[red]Error:[/red] {e}")
        return 1


def cmd_list(args: argparse.Namespace, config: FedLedgerConfig) -> int:
    """Execute the list command.
    
    Args:
        args: Parsed command-line arguments.
        config: Configuration object.
    
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    import pyarrow.parquet as pq
    
    # Find parquet files
    parquet_files = list(config.processed_dir.glob("*.parquet"))
    
    if not parquet_files:
        console.print("[yellow]No processed documents found[/yellow]")
        return 0
    
    # Filter by type if specified
    if args.type:
        parquet_files = [f for f in parquet_files if args.type in f.name]
    
    if args.format == "table":
        for parquet_file in parquet_files:
            table_data = pq.read_table(parquet_file)
            df = table_data.to_pandas()
            
            # Create rich table
            table = Table(title=f"Documents from {parquet_file.name}", show_header=True)
            
            # Add columns
            for col in ["doc_id", "title", "published_date", "doc_type"]:
                if col in df.columns:
                    table.add_column(col, style="cyan" if col == "doc_id" else None)
            
            # Add rows (limit to 20 for display)
            for _, row in df.head(20).iterrows():
                values = []
                for col in ["doc_id", "title", "published_date", "doc_type"]:
                    if col in df.columns:
                        val = row[col]
                        if col == "title" and val and len(str(val)) > 50:
                            val = str(val)[:47] + "..."
                        values.append(str(val) if val is not None else "")
                table.add_row(*values)
            
            console.print(table)
            console.print(f"[dim]Showing 20 of {len(df)} documents[/dim]\n")
    
    elif args.format == "json":
        for parquet_file in parquet_files:
            table_data = pq.read_table(parquet_file)
            df = table_data.to_pandas()
            print(df.to_json(orient="records", indent=2, date_format="iso"))
    
    elif args.format == "csv":
        for parquet_file in parquet_files:
            table_data = pq.read_table(parquet_file)
            df = table_data.to_pandas()
            print(df.to_csv(index=False))
    
    return 0


def cmd_info(args: argparse.Namespace, config: FedLedgerConfig) -> int:
    """Execute the info command.
    
    Args:
        args: Parsed command-line arguments.
        config: Configuration object.
    
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    # Search for document in metadata files
    metadata_files = list(config.metadata_dir.glob("*_metadata.json"))
    
    for metadata_file in metadata_files:
        with open(metadata_file, "r") as f:
            docs = json.load(f)
        
        for doc in docs:
            if doc.get("doc_id") == args.doc_id:
                # Display document info
                console.print(Panel.fit(
                    f"[bold cyan]Document Information[/bold cyan]\n\n"
                    f"[yellow]ID:[/yellow] {doc.get('doc_id')}\n"
                    f"[yellow]Type:[/yellow] {doc.get('doc_type')}\n"
                    f"[yellow]Title:[/yellow] {doc.get('title', 'N/A')}\n"
                    f"[yellow]Source URL:[/yellow] {doc.get('source_url')}\n"
                    f"[yellow]Published:[/yellow] {doc.get('published_date', 'N/A')}\n"
                    f"[yellow]Fetched:[/yellow] {doc.get('fetch_timestamp')}\n"
                    f"[yellow]Raw Path:[/yellow] {doc.get('raw_path')}",
                    title=f"Document {args.doc_id}"
                ))
                return 0
    
    console.print(f"[red]Document not found:[/red] {args.doc_id}")
    return 1


def cmd_stats(args: argparse.Namespace, config: FedLedgerConfig) -> int:
    """Execute the stats command.
    
    Args:
        args: Parsed command-line arguments.
        config: Configuration object.
    
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    import pyarrow.parquet as pq
    
    # Gather statistics
    parquet_files = list(config.processed_dir.glob("*.parquet"))
    
    if not parquet_files:
        console.print("[yellow]No processed documents found[/yellow]")
        return 0
    
    stats_table = Table(title="Archive Statistics", show_header=True)
    stats_table.add_column("Document Type", style="cyan")
    stats_table.add_column("Count", justify="right")
    stats_table.add_column("File Size", justify="right")
    
    total_docs = 0
    total_size = 0
    
    for parquet_file in parquet_files:
        table_data = pq.read_table(parquet_file)
        count = len(table_data)
        size = parquet_file.stat().st_size
        
        total_docs += count
        total_size += size
        
        doc_type = parquet_file.stem.replace("_documents", "")
        stats_table.add_row(
            doc_type,
            str(count),
            f"{size / 1024:.1f} KB"
        )
    
    console.print(stats_table)
    console.print(f"\n[bold]Total Documents:[/bold] {total_docs}")
    console.print(f"[bold]Total Storage:[/bold] {total_size / 1024:.1f} KB")
    
    return 0


def main(argv: Optional[list] = None) -> int:
    """Main entry point for the CLI.
    
    Args:
        argv: Command-line arguments. Uses sys.argv if None.
    
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    parser = setup_argparser()
    args = parser.parse_args(argv)
    
    # Show help if no command specified
    if not args.command:
        parser.print_help()
        return 0
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    setup_logging(level=log_level, json_output=args.log_json)
    
    # Create configuration
    config = FedLedgerConfig(
        data_dir=args.data_dir,
        log_level=log_level,
        log_json=args.log_json,
    )
    
    # Update config from sync-specific args
    if args.command == "sync":
        config.save_raw = args.save_raw
        config.parallel = args.parallel
        config.max_workers = args.workers
        config.overwrite = args.overwrite
    
    config.ensure_directories()
    
    # Dispatch to appropriate command handler
    command_handlers = {
        "sync": cmd_sync,
        "list": cmd_list,
        "info": cmd_info,
        "stats": cmd_stats,
    }
    
    handler = command_handlers.get(args.command)
    if not handler:
        console.print(f"[red]Error:[/red] Unknown command '{args.command}'")
        return 1
    
    try:
        return handler(args, config)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

