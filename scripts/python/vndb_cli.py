
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

import requests

# ---------------------------------------------------------------------------
# Third-party helpers (import-time validation only)
# ---------------------------------------------------------------------------
try:
    from vndb_client import (
        fetch_characters_by_vn_id,
        fetch_top_vns,
        fetch_vn_name_by_id,
    )
    from vndb_data_utils import (
        add_origin_to_character,
        clean_character_traits,
        create_records,
        normalize_birthday,
        normalize_image_url,
        normalize_origin_entry,
        normalize_sex,
        remove_duplicates,
        remove_duplicates_in_vns,
    )
except ImportError as exc:  # pragma: no cover - fail fast for missing deps.
    raise ImportError(
        "[vndb_cli] Mandatory dependency missing - did you add the project root to PYTHONPATH?"
    ) from exc

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_CHAR_BATCH: int = 25  # API courtesy delay after that many VNs
VN_DB_VERSION: str = "1.0.0"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"

# ---------------------------------------------------------------------------
# Logging bootstrap
# ---------------------------------------------------------------------------
LOGGER = logging.getLogger("vndb_cli")
LOGGER.setLevel(logging.INFO)
_HANDLER_STDERR = logging.StreamHandler(sys.stderr)
_HANDLER_STDERR.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt=LOG_DATEFMT)
)
LOGGER.addHandler(_HANDLER_STDERR)


def _install_file_logger(path: Path) -> None:
    """Add a second handler that writes to *path* (parent folders are created)."""

    path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt=LOG_DATEFMT)
    )
    LOGGER.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """Current local time in the project's canonical string format."""

    return time.strftime("%Y-%m-%d %H:%M:%S")


def _backoff_delay(attempt: int, base: float = 1.5) -> float:
    """Exponential back-off helper."""

    return base**attempt


def _sorted_by_id(objs: List[Dict]) -> List[Dict]:
    return sorted(objs, key=lambda o: int(o.get("id", "v0")[1:]))


# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

def fetch_characters(
    vn_ids: Iterable[str] | str,
    *,
    max_retries: int = 3,
    sleep_between_batches: int = 300,
) -> List[Dict]:
    """Fetch raw character dicts for one or more VN IDs with courteous rate-limiting.

    Parameters
    ----------
    vn_ids
        A single VN ID or an iterable of them (e.g. "v5").
    max_retries
        How many network retries per VN before giving up.
    sleep_between_batches
        Seconds to sleep **after** each *MAX_CHAR_BATCH* VNs.
    """

    ids: List[str] = [vn_ids] if isinstance(vn_ids, str) else list(vn_ids)

    LOGGER.info(
        "[vndb_cli] Sleeping %d s before first request to avoid VNDB rate-limits...",
        sleep_between_batches,
    )
    time.sleep(sleep_between_batches)

    characters: List[Dict] = []
    fetched_vns_since_pause = 0

    for vn_id in ids:
        LOGGER.debug("[vndb_cli] Fetching characters for %s", vn_id)
        time.sleep(5) 

        for attempt in range(max_retries + 1):
            try:
                batch = fetch_characters_by_vn_id(vn_id)
                if not batch:
                    LOGGER.warning("[vndb_cli] No characters found for %s", vn_id)
                    break

                for char in batch:
                    add_origin_to_character(char, vn_id)
                characters.extend(batch)
                fetched_vns_since_pause += 1
                break

            except requests.exceptions.RequestException as err:
                if attempt >= max_retries:
                    raise RuntimeError(
                        f"[vndb_cli] Giving up on {vn_id} after {max_retries} attempts"
                    ) from err

                delay = _backoff_delay(attempt)
                LOGGER.info("[vndb_cli] %s - retry in %.1fs...", err, delay)
                time.sleep(delay)

        if fetched_vns_since_pause >= MAX_CHAR_BATCH:
            LOGGER.info(
                "[vndb_cli] Processed %d VNs - long nap (%ds)...",
                fetched_vns_since_pause,
                sleep_between_batches,
            )
            time.sleep(sleep_between_batches)
            fetched_vns_since_pause = 0

    return characters


# ---------------------------------------------------------------------------
# Database helpers (JSON I/O)
# ---------------------------------------------------------------------------

def _initial_db(top_vns: List[Dict]) -> Dict[str, object]:
    return {"characters": [], "top_vns": top_vns, "version": VN_DB_VERSION, "date": _now_iso()}


def _read_json(path: Path) -> Dict:
    return json.loads(path.read_text("utf-8")) if path.exists() else {}


def _write_json(path: Path, data: Dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


# ---------------------------------------------------------------------------
# Side-effecting pipeline steps
# ---------------------------------------------------------------------------

def prepare_records(raw_chars: List[Dict]) -> List[Dict]:
    """Run the same normalisation pipeline the original script used."""

    LOGGER.info("[vndb_cli] Removing duplicates...")
    chars = remove_duplicates(raw_chars)

    records = create_records(chars)

    LOGGER.info("[vndb_cli] Cleaning + normalising ...")
    pipeline: List[Callable[[Dict], Dict]] = [
        clean_character_traits,
        normalize_birthday,
        normalize_image_url,
        normalize_sex,
        normalize_origin_entry,
    ]
    for func in pipeline:
        records = list(map(func, records))

    return records


# ---------------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------------

def _parse_cli(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="vndb_cli",
        description="Fetch VNDB character data and optionally assign non-unique traits.",
    )

    parser.add_argument("--vn_ids", "-v", nargs="+", help="One or more VN IDs, e.g. v5.")
    parser.add_argument("--out", "-o", type=Path, help="Write output to file instead of stdout.")
    parser.add_argument("--raw", action="store_true", help="Dump raw JSON and exit.")
    parser.add_argument("--version", action="version", version=f"vndb_cli {VN_DB_VERSION}")

    parser.add_argument("--top-vns", "-t", type=int, default=0, help="Fetch top N VNs by rating.")
    parser.add_argument("--start", type=int, default=1, help="Start page when using --top-vns.")
    parser.add_argument("--sort", default="votecount", help="Sort criterion for top VNs.")

    parser.add_argument("--sleep", type=float, default=300, help="Seconds to sleep after each batch.")

    parser.add_argument("--logfile", type=Path, help="Enable file logging (path cannot exist).")
    parser.add_argument("--append", action="store_true", help="Append to existing --out JSON DB.")
    return parser.parse_args(argv)


def _unique(seq: Iterable) -> List:  # preserves order
    seen = set()
    out = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _cli(argv: Optional[List[str]] = None) -> None:  # noqa: C901 - core orchestrator
    args = _parse_cli(argv)

    if args.logfile:
        _install_file_logger(args.logfile)
        LOGGER.info("[vndb_cli] File logging → %s", args.logfile)

    # ------------------------------------------------------------------
    # 1. Build list of VN IDs (direct or top-VN lookup)
    # ------------------------------------------------------------------
    vn_ids: List[str] = []
    top_vns: List[Dict] = []

    if args.vn_ids:
        vn_ids.extend(args.vn_ids)
        top_vns.extend(fetch_vn_name_by_id(i) for i in args.vn_ids)

    if args.top_vns:
        LOGGER.info("[vndb_cli] Fetching top %d VNs ...", args.top_vns)
        top_vns_batch = fetch_top_vns(args.top_vns, args.start, args.sort)
        if not top_vns_batch:
            LOGGER.error("[vndb_cli] No top VNs returned - exiting.")
            sys.exit(1)
        top_vns.extend(top_vns_batch)
        vn_ids.extend(vn["id"] for vn in top_vns_batch)

    # de-dup while preserving CLI order (explicit IDs first)
    vn_ids = _unique(vn_ids)

    if not vn_ids:
        LOGGER.error("[vndb_cli] No VN IDs given (use --vn_ids or --top-vns).")
        sys.exit(1)

    # remove vns that are already in the DB
    if args.out and args.append:
        existing_db = _read_json(args.out)
        existing_ids = {rec["id"] for rec in existing_db.get("top_vns", [])}
        vn_ids = [vn_id for vn_id in vn_ids if vn_id not in existing_ids]
        LOGGER.info("[vndb_cli] %d VNs missing in current DB.", len(vn_ids))
    
    if not vn_ids:
        LOGGER.error("[vndb_cli] No VNs to fetch - exiting.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 2. Network fetch
    # ------------------------------------------------------------------
    eta_sec = len(vn_ids) * 5 + (len(vn_ids) // MAX_CHAR_BATCH + 1) * args.sleep
    LOGGER.info("[vndb_cli] Fetching characters for %d VNs (≈ %.1f min)...", len(vn_ids), eta_sec / 60)

    raw_chars = fetch_characters(vn_ids, sleep_between_batches=int(args.sleep))
    if not raw_chars:
        LOGGER.error("[vndb_cli] No characters fetched - exiting.")
        sys.exit(1)

    LOGGER.info("[vndb_cli] Fetched %d raw characters.", len(raw_chars))

    if args.raw:
        payload = json.dumps(raw_chars, ensure_ascii=False, indent=2)
        (args.out.write_text(payload, "utf-8") if args.out else print(payload))
        return

    # ------------------------------------------------------------------
    # 3. Normalise & label
    # ------------------------------------------------------------------
    records = prepare_records(raw_chars)
    LOGGER.info("[vndb_cli] Normalised → %d unique characters.", len(records))

    # ------------------------------------------------------------------
    # 4. Persist / merge
    # ------------------------------------------------------------------
    if args.out:
        existing_db = _read_json(args.out) if args.append else {}

        merged_records = (
            remove_duplicates(existing_db.get("characters", []) + records) if args.append else records
        )
        LOGGER.info(
            "[vndb_cli] Merged %d existing + %d new records → %d total",
            len(existing_db.get("characters", [])),
            len(records),
            len(merged_records),
        )
        merged_records = _sorted_by_id(merged_records)

        merged_top_vns = remove_duplicates_in_vns(existing_db.get("top_vns", []) + top_vns) if args.append else top_vns
        merged_top_vns = _sorted_by_id(merged_top_vns)

        db_payload = {
            "characters": merged_records,
            "top_vns": merged_top_vns,
            "version": VN_DB_VERSION,
            "date": _now_iso(),
        }
        _write_json(args.out, db_payload)
        LOGGER.info("[vndb_cli] wrote %d records → %s", len(merged_records), args.out)
        LOGGER.info("[vndb_cli] wrote %d top VNs → %s", len(merged_top_vns), args.out)
    else:
        for rec in records:
            print(json.dumps(rec, ensure_ascii=False))