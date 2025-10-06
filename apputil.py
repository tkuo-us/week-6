# your code here ...
from __future__ import annotations

import os
import time
import typing as t
import requests
import pandas as pd


class Genius:
    """
    Minimal Genius API wrapper for the week06 exercises.

    Usage:
        from apputil import Genius
        g = Genius(access_token="YOUR_TOKEN")
        info = g.get_artist("Radiohead")     # -> dict (artist JSON)
        df   = g.get_artists(["Rihanna", "Tycho", "Seal", "U2"])  # -> DataFrame
    """

    BASE_URL = "https://api.genius.com"

    def __init__(self, access_token: str | None = None, *, timeout: int = 20):
        # allow token
        token = access_token or os.environ.get("GENIUS_ACCESS_TOKEN")
        if not token:
            raise ValueError(
                "Genius access_token is required. "
                "Pass access_token=... or set GENIUS_ACCESS_TOKEN env var."
            )
        self.access_token = token
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {self.access_token}"})

    # -------- internal helpers --------
    def _get(self, path: str, params: dict | None = None) -> dict:
        """Low-level GET with error handling; returns parsed JSON (dict)."""
        url = path if path.startswith("http") else f"{self.BASE_URL}{path}"
        r = self._session.get(url, params=params or {}, timeout=self.timeout)
        if r.status_code != 200:
            raise requests.HTTPError(
                f"GET {url} failed with status={r.status_code}: {r.text[:200]}"
            )
        data = r.json()
        # Genius API include response, keep fully JSON
        return data

    def _response_field(self, data: dict, key: str, default=None):
        """Convenience to fetch data['response'][key] with a default."""
        return (data.get("response") or {}).get(key, default)

    # -------- public API for the exercises --------
    def get_artist(self, search_term: str) -> dict:
        """
        Exercise 2:
        1) search search_term
        2) getting first primary_artist.id
        3) using /artists/<id> to get artist detail JSON
        4) return artist dict (data['response']['artist'])
        """
        if not search_term or not isinstance(search_term, str):
            raise ValueError("search_term must be a non-empty string")

        # 1) search
        search = self._get("/search", params={"q": search_term})
        hits: list[dict] = self._response_field(search, "hits", default=[]) or []
        if not hits:
            # return empty dict
            return {}

        # 2) primary artist id from first hit
        first = hits[0].get("result") or {}
        primary = first.get("primary_artist") or {}
        artist_id = primary.get("id")
        if not artist_id:
            return {}

        # 3) fetch artist detail
        artist_json = self._get(f"/artists/{artist_id}")
        artist = self._response_field(artist_json, "artist", default={}) or {}

        # 4) return artist dict
        return artist

    def get_artists(self, search_terms: list[str]) -> pd.DataFrame:
        """
        Exercise 3:
        call get_artist from search string, gen to DataFrame
        columns: search_term, artist_name, artist_id, followers_count
        """
        if not isinstance(search_terms, (list, tuple)):
            raise ValueError("search_terms must be a list/tuple of strings")

        rows: list[dict] = []
        for term in search_terms:
            artist = self.get_artist(term)
            rows.append(
                {
                    "search_term": term,
                    "artist_name": artist.get("name"),
                    "artist_id": artist.get("id"),
                    # return None if the account doesn't followers_count
                    "followers_count": artist.get("followers_count"),
                }
            )
            # avoid deny
            time.sleep(0.1)

        return pd.DataFrame(rows)
