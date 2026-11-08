"""JSON veilig vervangen: een onderbroken schrijfactie beschadigt de dataset niet."""

import json
import os
import tempfile
from pathlib import Path


def schrijf_json(pad, inhoud):
    pad = Path(pad)
    pad.parent.mkdir(parents=True, exist_ok=True)
    tijdelijk = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=pad.parent,
            prefix=f".{pad.name}.", suffix=".tmp", delete=False,
        ) as bestand:
            tijdelijk = Path(bestand.name)
            json.dump(inhoud, bestand, ensure_ascii=False, indent=2, allow_nan=False)
            bestand.write("\n")
        os.replace(tijdelijk, pad)
    finally:
        if tijdelijk is not None:
            tijdelijk.unlink(missing_ok=True)
