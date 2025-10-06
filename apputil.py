# your code here ...
from __future__ import annotations

import os
import time
import typing as t
import requests
import pandas as pd

import string
from typing import Iterable, Dict, Any
from concurrent.futures import ProcessPoolExecutor, as_completed



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

    def __init__(self, access_token: str | None = None, *, timeout: int = 20, per_call_sleep: float = 0.05):
        # allow token
        token = access_token or os.environ.get("GENIUS_ACCESS_TOKEN")
        if not token:
            raise ValueError(
                "Genius access_token is required. "
                "Pass access_token=... or set GENIUS_ACCESS_TOKEN env var."
            )
        self.access_token = token
        self._token = token
        self.timeout = timeout
        self.per_call_sleep = per_call_sleep
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
    
    # ---------- Bonus 1: Gather a list of 100+ various musical artists ----------
    def collect_artist_names(
        self,
        seeds: Iterable[str] | None = None,
        *,
        target: int = 120,
        per_page: int = 50, 
        max_pages: int = 10, 
        out_txt: str | None = None,
    ) -> list[str]:
        """
        Using /search?q=<seed> gather primary artist name
        - seeds: a-z/0-9
        - target: at least
        - per_page/max_pages: change to next page
        """
        if seeds is None:
            seeds = list("abcdefghijklmnopqrstuvwxyz")

        names: dict[str, None] = {}

        for seed in seeds:
            for page in range(1, max_pages + 1):
                try:
                    data = self._get("/search", params={"q": seed, "page": page, "per_page": per_page})
                    hits: list[dict] = self._response_field(data, "hits", default=[]) or []
                    if not hits:
                        break

                    for h in hits:
                        primary = (h.get("result") or {}).get("primary_artist") or {}
                        name = (primary.get("name") or "").strip()
                        if name:
                            names[name] = None

                    if self.per_call_sleep > 0:
                        time.sleep(self.per_call_sleep)

                except Exception:
                    # check
                    continue

            if len(names) >= target:
                break

        out = sorted(names.keys())
        if out_txt:
            self.save_list(out, out_txt)
        return out

    def get_artists_mp(self, search_terms: Iterable[str], workers: int = 8) -> pd.DataFrame:
        terms = [str(t).strip() for t in search_terms if str(t).strip()]
        rows: list[dict] = []
        if not terms:
            return pd.DataFrame(rows)

        # Using: access_token / timeout / per_call_sleep
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(_mp_fetch_one, term, self.access_token, self.timeout, self.per_call_sleep)
                for term in terms
            ]
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    if res:
                        rows.append(res)
                except Exception as e:
                    rows.append({"search_term": None, "artist_name": None, "artist_id": None, "followers_count": None, "_error": str(e)})

        return pd.DataFrame(rows)

    @staticmethod
    def save_list(items: Iterable[str], path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            for x in items:
                f.write(f"{x}\n")

    @staticmethod
    def save_df(df: pd.DataFrame, path: str) -> None:
        df.to_csv(path, index=False)


# module-level worker for multiprocessing
def _mp_fetch_one(term: str, token: str, timeout: float, per_call_sleep: float) -> Dict[str, Any]:
    try:
        g = Genius(access_token=token, timeout=timeout, per_call_sleep=per_call_sleep)
        artist = g.get_artist(term)
        return {
            "search_term": term,
            "artist_name": artist.get("name"),
            "artist_id": artist.get("id"),
            "followers_count": artist.get("followers_count"),
        }
    except Exception as e:
        return {
            "search_term": term,
            "artist_name": None,
            "artist_id": None,
            "followers_count": None,
            "_error": str(e),
        }
