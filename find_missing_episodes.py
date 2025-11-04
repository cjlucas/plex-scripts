#!/usr/bin/env python3
"""
Script to find missing episodes in Plex by comparing with TVDB data.
Requires PLEX_URL, PLEX_TOKEN, and TVDB_API_KEY environment variables.
"""

import os
import sys
import csv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from plexapi.server import PlexServer
import requests


def print_stderr(*args, **kwargs):
    """Print to stderr"""
    print(*args, file=sys.stderr, **kwargs)


def print_stdout(*args, **kwargs):
    """Print to stdout"""
    print(*args, file=sys.stdout, **kwargs)


class TVDBClient:
    """Client for TVDB API v4"""

    BASE_URL = "https://api4.thetvdb.com/v4"

    def __init__(self, api_key, pin=None):
        self.api_key = api_key
        self.pin = pin
        self.token = None
        self._authenticate()

    def _authenticate(self):
        """Authenticate with TVDB and get access token"""
        # Build auth payload
        auth_data = {"apikey": self.api_key}
        if self.pin:
            auth_data["pin"] = self.pin

        try:
            response = requests.post(
                f"{self.BASE_URL}/login",
                json=auth_data
            )
            response.raise_for_status()
            self.token = response.json()["data"]["token"]
        except requests.exceptions.HTTPError as e:
            # Print the actual error message from TVDB
            error_detail = ""
            try:
                error_data = response.json()
                error_detail = f"\nTVDB Error: {error_data}"
            except:
                error_detail = f"\nResponse: {response.text}"

            print_stderr(f"\nAuthentication failed: {e}{error_detail}")
            print_stderr("\nNote: Some TVDB API keys require a PIN.")
            print_stderr("Check your API key settings at: https://thetvdb.com/dashboard/account/apikeys")
            raise

    def _get_headers(self):
        """Get headers with authentication token"""
        return {"Authorization": f"Bearer {self.token}"}

    def search_series(self, name):
        """Search for a series by name"""
        try:
            response = requests.get(
                f"{self.BASE_URL}/search",
                headers=self._get_headers(),
                params={"query": name, "type": "series"},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            if not data.get("data"):
                return []

            return data["data"]
        except Exception as e:
            print_stderr(f"  Warning: TVDB search failed: {e}")
            return []

    def get_series_episodes(self, series_id):
        """Get all episodes for a series"""
        episodes = []
        page = 0

        while True:
            try:
                response = requests.get(
                    f"{self.BASE_URL}/series/{series_id}/episodes/default",
                    headers=self._get_headers(),
                    params={"page": page},
                    timeout=10
                )
                response.raise_for_status()
                data = response.json()

                if not data.get("data", {}).get("episodes"):
                    break

                episodes.extend(data["data"]["episodes"])

                # Check if there are more pages
                links = data.get("links", {})
                if not links.get("next"):
                    break

                page += 1
            except Exception as e:
                print_stderr(f"  Warning: Failed to fetch episodes page {page}: {e}")
                break

        return episodes


def get_plex_episodes(show):
    """Get all episodes from Plex for a given show"""
    # Get all episodes organized by season
    episodes_by_season = defaultdict(set)

    for episode in show.episodes():
        season_num = episode.seasonNumber
        episode_num = episode.episodeNumber

        # Skip specials (season 0) for now
        if season_num is not None and episode_num is not None and season_num > 0:
            episodes_by_season[season_num].add(episode_num)

    return episodes_by_season


def get_tvdb_episodes(tvdb, show_name, show_year=None):
    """Get all episodes from TVDB for a given show"""
    # Search for the series
    results = tvdb.search_series(show_name)

    if not results:
        return None

    # Try to find best match based on year if available
    series = results[0]
    if show_year and len(results) > 1:
        for result in results:
            if result.get("year") and str(result.get("year")) == str(show_year):
                series = result
                break

    # Get all episodes
    episodes = tvdb.get_series_episodes(series["tvdb_id"])

    # Organize by season and episode number with full episode data
    episodes_by_season = defaultdict(dict)

    for episode in episodes:
        season_num = episode.get("seasonNumber")
        episode_num = episode.get("number")

        # Skip specials and episodes without proper numbering
        if season_num and episode_num and season_num > 0:
            episodes_by_season[season_num][episode_num] = {
                'title': episode.get('name', 'Unknown Title'),
                'aired': episode.get('aired', ''),
            }

    return episodes_by_season


def find_missing_episodes(plex_episodes, tvdb_episodes):
    """Compare Plex and TVDB episodes to find what's missing"""
    missing = defaultdict(dict)

    # Check each season in TVDB
    for season_num in sorted(tvdb_episodes.keys()):
        tvdb_eps = tvdb_episodes[season_num]  # dict of {episode_num: {title, aired}}
        plex_eps = plex_episodes.get(season_num, set())  # set of episode numbers

        # Find missing episodes
        for episode_num, episode_data in tvdb_eps.items():
            if episode_num not in plex_eps:
                missing[season_num][episode_num] = episode_data

    return missing


def format_episode_ranges(episodes):
    """Format episode numbers into ranges (e.g., '1-5, 7, 9-11')"""
    if not episodes:
        return ""

    ranges = []
    start = episodes[0]
    end = episodes[0]

    for ep in episodes[1:]:
        if ep == end + 1:
            end = ep
        else:
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = end = ep

    # Add the last range
    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")

    return ', '.join(ranges)


def check_show(tvdb, show, progress_lock, current, total):
    """Check a single show for missing episodes"""
    show_name = show.title
    show_year = show.year if hasattr(show, 'year') else None

    # Get episodes from Plex
    plex_episodes = get_plex_episodes(show)

    # Get episodes from TVDB
    tvdb_episodes = get_tvdb_episodes(tvdb, show_name, show_year)

    if not tvdb_episodes:
        with progress_lock:
            print_stderr(f"[{current}/{total}] {show_name}... ⚠ Not found on TVDB")
        return show_name, None

    # Find missing episodes
    missing = find_missing_episodes(plex_episodes, tvdb_episodes)

    # Print progress
    with progress_lock:
        if missing:
            # Count total missing episodes across all seasons
            total_missing = sum(len(season_eps) for season_eps in missing.values())
            print_stderr(f"[{current}/{total}] {show_name}... ✗ {total_missing} missing")
        else:
            print_stderr(f"[{current}/{total}] {show_name}... ✓")

    return show_name, missing if missing else None


def main():
    # Check for required environment variables
    plex_url = os.environ.get("PLEX_URL")
    plex_token = os.environ.get("PLEX_TOKEN")
    tvdb_api_key = os.environ.get("TVDB_API_KEY")
    tvdb_pin = os.environ.get("TVDB_PIN")  # Optional PIN for subscriber keys

    if not plex_url or not plex_token:
        print_stderr("Error: PLEX_URL and PLEX_TOKEN environment variables are required")
        sys.exit(1)

    if not tvdb_api_key:
        print_stderr("Error: TVDB_API_KEY environment variable is required")
        print_stderr("Get your API key from: https://thetvdb.com/dashboard/account/apikeys")
        sys.exit(1)

    # Connect to Plex
    print_stderr("Connecting to Plex...")
    plex = PlexServer(plex_url, plex_token)

    # Connect to TVDB
    print_stderr("Connecting to TVDB...")
    tvdb = TVDBClient(tvdb_api_key, tvdb_pin)

    # Get all TV shows from Plex
    print_stderr("\nFetching TV shows from Plex...")
    tv_section = plex.library.section("TV Shows")
    all_shows = tv_section.all()
    print_stderr(f"Found {len(all_shows)} shows in your library")

    # Check each show for missing episodes (in parallel)
    shows_with_missing = {}
    print_stderr("\nChecking shows for missing episodes (parallel processing)...")
    print_stderr("(This may take a while for large libraries)\n")

    progress_lock = Lock()
    max_workers = 10  # Number of concurrent TVDB API requests

    # Create a list of tasks with their index
    tasks = [(i, show) for i, show in enumerate(all_shows, 1)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_show = {
            executor.submit(check_show, tvdb, show, progress_lock, i, len(all_shows)): show
            for i, show in tasks
        }

        # Collect results as they complete
        for future in as_completed(future_to_show):
            show_name, missing = future.result()
            if missing:
                shows_with_missing[show_name] = missing

    # Generate CSV output
    print_stderr("\n" + "="*70)
    print_stderr("GENERATING REPORT")
    print_stderr("="*70)

    total_shows_missing = len(shows_with_missing)
    total_episodes_missing = sum(
        sum(len(season_eps) for season_eps in missing.values())
        for missing in shows_with_missing.values()
    ) if shows_with_missing else 0

    print_stderr(f"\nShows with missing episodes: {total_shows_missing}/{len(all_shows)}")
    print_stderr(f"Total missing episodes: {total_episodes_missing}")
    print_stderr("")

    # Write CSV to stdout - one row per episode
    writer = csv.writer(sys.stdout)
    writer.writerow(['Show Name', 'Season', 'Episode', 'Episode Title', 'Air Date'])

    if shows_with_missing:
        for show_name in sorted(shows_with_missing.keys()):
            missing = shows_with_missing[show_name]

            for season_num in sorted(missing.keys()):
                episodes = missing[season_num]  # dict of {episode_num: {title, aired}}

                for episode_num in sorted(episodes.keys()):
                    episode_data = episodes[episode_num]
                    writer.writerow([
                        show_name,
                        season_num,
                        episode_num,
                        episode_data['title'],
                        episode_data['aired']
                    ])

    print_stderr("\n" + "="*70)
    print_stderr("✓ CSV output complete")
    print_stderr("="*70)


if __name__ == "__main__":
    main()
