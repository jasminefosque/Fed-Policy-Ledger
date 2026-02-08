"""HTTP client utilities for fetching Federal Reserve documents.

This module provides a robust HTTP client with retry logic, disk caching,
and proper error handling for retrieving documents from Federal Reserve
websites.
"""

import time
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse
import hashlib

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    requests = None
    HTTPAdapter = None
    Retry = None


class HTTPSession:
    """HTTP session with retry logic and disk caching.
    
    Provides a configured session for fetching documents with automatic
    retries on transient failures and optional disk caching to avoid
    redundant requests.
    
    Attributes:
        cache_dir: Directory for caching HTTP responses.
        session: Underlying requests.Session object.
        timeout: Default timeout for requests in seconds.
        user_agent: User-Agent string for requests.
    """
    
    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 0.3,
        user_agent: Optional[str] = None
    ):
        """Initialize HTTP session with retry and caching configuration.
        
        Args:
            cache_dir: Directory for caching responses. If None, caching is disabled.
            timeout: Request timeout in seconds.
            max_retries: Maximum number of retry attempts for failed requests.
            backoff_factor: Backoff factor for retry delays (delay = backoff * (2 ^ retry_count)).
            user_agent: Custom User-Agent string. Defaults to a descriptive agent.
        """
        if requests is None:
            raise ImportError(
                "requests library is required. Install with: pip install requests"
            )
        
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.timeout = timeout
        self.user_agent = user_agent or "FedPolicyLedger/0.1.0 (Research/Archival)"
        
        # Initialize cache directory if caching is enabled
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure session with retry strategy
        self.session = requests.Session()
        
        # Set up retry strategy for common transient failures
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/pdf,*/*",
        })
    
    def get(
        self,
        url: str,
        use_cache: bool = True,
        timeout: Optional[int] = None,
        **kwargs
    ) -> "requests.Response":
        """Fetch a URL with optional disk caching.
        
        Args:
            url: URL to fetch.
            use_cache: Whether to use cached response if available.
            timeout: Request timeout in seconds. Uses session default if None.
            **kwargs: Additional arguments passed to requests.get().
        
        Returns:
            Response object from requests.
        
        Raises:
            requests.RequestException: If the request fails after retries.
        """
        # Check cache first if enabled
        if use_cache and self.cache_dir:
            cached_response = self._get_from_cache(url)
            if cached_response:
                return cached_response
        
        # Perform the request
        timeout = timeout or self.timeout
        response = self.session.get(url, timeout=timeout, **kwargs)
        response.raise_for_status()
        
        # Cache the response if caching is enabled
        if self.cache_dir:
            self._save_to_cache(url, response)
        
        return response
    
    def _get_cache_path(self, url: str) -> Path:
        """Generate cache file path for a URL.
        
        Args:
            url: URL to generate cache path for.
        
        Returns:
            Path object for the cache file.
        """
        # Use URL hash as filename to avoid filesystem issues
        url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{url_hash}.cache"
    
    def _get_from_cache(self, url: str) -> Optional[Any]:
        """Retrieve cached response for a URL.
        
        Args:
            url: URL to check cache for.
        
        Returns:
            Cached response if available, None otherwise.
        """
        cache_path = self._get_cache_path(url)
        
        if not cache_path.exists():
            return None
        
        # For now, we don't implement full response caching
        # This is a placeholder for future implementation
        return None
    
    def _save_to_cache(self, url: str, response: "requests.Response") -> None:
        """Save response to disk cache.
        
        Args:
            url: URL of the response.
            response: Response object to cache.
        """
        # Placeholder for cache implementation
        # In a full implementation, this would serialize the response
        pass
    
    def close(self):
        """Close the HTTP session and release resources."""
        if self.session:
            self.session.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def download_to_file(
    url: str,
    output_path: Path,
    session: Optional[HTTPSession] = None,
    chunk_size: int = 8192
) -> Dict[str, Any]:
    """Download a URL to a file with streaming support.
    
    Args:
        url: URL to download.
        output_path: Path where the file should be saved.
        session: Optional HTTPSession to use. Creates a new one if None.
        chunk_size: Size of chunks for streaming download.
    
    Returns:
        Dictionary with download metadata including status_code, content_type,
        and file size.
    
    Raises:
        requests.RequestException: If download fails.
    """
    own_session = False
    if session is None:
        session = HTTPSession()
        own_session = True
    
    try:
        response = session.get(url, stream=True)
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Stream download to file
        total_size = 0
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
        
        return {
            "status_code": response.status_code,
            "content_type": response.headers.get("Content-Type", ""),
            "size_bytes": total_size,
            "output_path": str(output_path)
        }
    
    finally:
        if own_session:
            session.close()
