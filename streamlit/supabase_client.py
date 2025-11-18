"""Supabase client wrapper for Streamlit usage."""
import os
from typing import Optional

from supabase import Client, create_client

_client: Optional[Client] = None


def _build_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    anon_key = os.environ.get("SUPABASE_ANON_KEY")

    if not url or not anon_key:
        missing = []
        if not url:
            missing.append("SUPABASE_URL")
        if not anon_key:
            missing.append("SUPABASE_ANON_KEY")
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    return create_client(url, anon_key)


def get_supabase() -> Client:
    """Return singleton Supabase client instance."""
    global _client
    if _client is None:
        _client = _build_client()
    return _client
