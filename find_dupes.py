import os

from plexapi.server import PlexServer

plex = PlexServer(os.environ.get("PLEX_URL"), os.environ.get("PLEX_TOKEN"))

dupes_found = 0
for movie in plex.library.section('Movies').search():
    parts = list(movie.iterParts())
    if len(parts) > 1:
        dupes_found += 1
        for part in parts:
            print(part.file)

print(f"Duplicates found: {dupes_found}")
