from __future__ import annotations

from collections import Counter
from typing import Dict, List

__all__ = [
    "NSFW_GROUP_SUBSTRINGS",
    "GROUP_WEIGHTS",
    "is_sfw",
    "collect_frequencies",
    "score_trait",
    "select_signature_trait",
    "create_records",
    "remove_duplicates",
    "add_origin_to_character",
]

# ---------------------------------------------------------------------------
# Configurable constants
# ---------------------------------------------------------------------------

NSFW_GROUP_SUBSTRINGS: tuple[str, ...] = (
    "(Sexual)",
)

GROUP_WEIGHTS: Dict[str, float] = {
    "Personality": 1.4,
    "Role": 1.2,
    "Engages in": 1.0,
    "Subject of": 1.0,
}
_DEFAULT_WEIGHT = 0.8  # for groups not in dict above

# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def is_sfw(trait: Dict) -> bool:
    """Return *True* if the trait’s group is **not** flagged NSFW."""
    return not any(bad in trait.get("group_name", "") for bad in NSFW_GROUP_SUBSTRINGS)


def collect_frequencies(characters: List[Dict]) -> Counter:
    """Return Counter mapping trait-id → number of characters that have it (SFW only)."""
    freq: Counter = Counter()
    for ch in characters:
        for tr in ch.get("traits", []):
            if is_sfw(tr):
                freq[tr["id"]] += 1
    return freq


def score_trait(trait: Dict, freq: Counter) -> float:
    """Information-gain style score (rarer + weighted groups ⇒ higher)."""
    trait_id = trait["id"]
    base = 1.0 / freq[trait_id] if freq[trait_id] else 0.0
    weight = GROUP_WEIGHTS.get(trait.get("group_name", ""), _DEFAULT_WEIGHT)
    return base * weight


def _best_trait(traits: List[Dict], freq: Counter) -> str | None:
    best_score = -1.0
    best_name: str | None = None
    for tr in traits:
        sc = score_trait(tr, freq)
        if sc > best_score:
            best_score = sc
            best_name = tr["name"]
    return best_name


def select_signature_trait(character: Dict,
                           freq: Counter,
                           *,
                           min_overlap: int = 2) -> str | None:
    """Return a signature trait name that meets the *overlap* criterion.

    The function first filters for SFW traits that appear in **at least**
    `min_overlap` characters across the roster. Among those it picks the one
    with the highest `score_trait` value. If the character has *no* qualifying
    trait, it falls back to the globally best-scoring SFW trait (even if
    unique) so every character still gets a label.
    """
    # Split traits into SFW + overlap-eligible vs rest
    overlap_ok: List[Dict] = []
    leftovers: List[Dict] = []
    for tr in character.get("traits", []):
        if not is_sfw(tr):
            continue
        (overlap_ok if freq[tr["id"]] >= min_overlap else leftovers).append(tr)

    # Try to pick from the overlap-eligible pool first
    chosen = _best_trait(overlap_ok, freq) if overlap_ok else None
    if chosen is not None:
        return chosen

    # Fallback: pick the highest-scoring SFW trait regardless of overlap
    return _best_trait(leftovers, freq) or None

def create_records(characters: List[Dict]) -> List[Dict]:
    freq = collect_frequencies(characters)

    def _label_record(ch: Dict) -> Dict[str, str | None]:
        sig = select_signature_trait(ch, freq, min_overlap=2)
        if sig is None:
            sig = next((t["name"] for t in ch.get("traits", []) if is_sfw(t)), None)
        ch["non_unique_trait"] = sig
        return ch
    
    return [_label_record(ch) for ch in characters]

def remove_duplicates(records: List[Dict]) -> List[Dict]:
    """Remove duplicate records based on 'id' and 'name'."""
    seen = set()
    unique_records = []
    for record in records:
        identifier = (record["id"], record["name"])
        if identifier not in seen:
            seen.add(identifier)
            unique_records.append(record)
    return unique_records

def remove_duplicates_in_vns(vns: List[Dict]) -> List[Dict]:
    """Remove duplicate VN entries based on 'id'."""
    seen = set()
    unique_vns: List[Dict] = []
    for vn in vns:
        if vn["id"] not in seen:
            seen.add(vn["id"])
            unique_vns.append(vn)
    return unique_vns

def add_origin_to_character(character: Dict, origin: str) -> Dict:
    """Add an 'origin' field to the character dict."""
    character["origin"] = origin
    return character

from collections import defaultdict
from typing import Dict, List, Any

def clean_character_traits(char: Dict[str, Any]) -> Dict[str, Any]:
    """
    Replace the ``traits`` list in a VNDB-style character object with
    five explicit fields—Hair, Personality, Eyes, Role, Body—whose values
    are the *names* of the traits that belong to each group.

    If a group has…
      • exactly one match → the value is a string
      • multiple matches  → the value is a list of strings
      • no matches        → the field is omitted

    The function returns a **new** dict and leaves the input untouched.
    """
    desired_groups = {"Hair", "Personality", "Eyes", "Role", "Body"}

    # Collect names by group
    collected: Dict[str, List[str]] = defaultdict(list)
    for t in char.get("traits", []):
        group = t.get("group_name")
        if group in desired_groups:
            collected[group].append(t.get("name"))

    # Build result
    cleaned = {k: v for k, v in char.items() if k != "traits"}
    for group in desired_groups:
        names = collected.get(group)
        if not names:                      # skip empty groups
            continue
        names.sort()                  # sort names alphabetically
        cleaned[group] = names

    return cleaned

from typing import Dict, Any, List

def birthday_to_string(bday: List[int]) -> str:
    """
    Convert a `[month, day]` list to the string ``"DD.MM."``.
    Example: [8, 30] → "30.08."
    """
    if len(bday) != 2:
        raise ValueError("Birthday must be [month, day].")
    month, day = bday
    return f"{day:02d}.{month:02d}."

def normalize_birthday(char: Dict[str, Any]) -> Dict[str, Any]:
    """
    Replace the ``birthday`` list in *char* with its formatted string.
    Returns the same dict for convenience (edits in-place).
    """
    if isinstance(char.get("birthday"), list):
        char["birthday"] = birthday_to_string(char["birthday"])
    return char

from typing import Dict, Any

def normalize_image_url(char: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert
        "image": { "url": "<link>" }
    into
        "image_url": "<link>"

    The function returns a *new* dict so your original stays pristine.
    """
    cleaned = char.copy()                     # shallow copy
    img = cleaned.pop("image", None)          # remove 'image' if present
    if isinstance(img, dict) and "url" in img:
        cleaned["image_url"] = img["url"]
    return cleaned

from typing import Dict, Any

def normalize_sex(char: Dict[str, Any]) -> Dict[str, Any]:
    """
    If ``char["sex"]`` is a list, replace it with its first element.
    Returns a *new* dict, leaving the original untouched.
    """
    cleaned = char.copy()             # shallow copy
    sex_field = cleaned.get("sex")
    if isinstance(sex_field, list) and sex_field:
        cleaned["sex"] = sex_field[0]
    return cleaned

def normalize_origin_entry(char: Dict[str, Any]) -> Dict[str, Any]:
    """
    Locate the VN inside ``char["vns"]`` whose ``id`` equals ``char["origin"]``.
    From that entry, copy:
        * released  → char["released"]
        * role      → char["role"]
        * developers[0]["name"] → char["developer"]

    Returns a *new* dict, leaving the original untouched.
    If the origin entry isn’t found, the character data is returned unchanged.
    """
    origin_id = char.get("origin")
    if not origin_id:
        return char

    new_char = char.copy()                      # shallow copy
    for vn in char.get("vns", []):
        if vn.get("id") == origin_id:
            new_char["released"] = vn.get("released")
            new_char["role"] = vn.get("role")
            devs = vn.get("developers", [])
            if devs:
                new_char["developer"] = devs[0].get("name")
            break                               # found the match—stop looking
    new_char.pop("vns", None)  # remove 'vns' if present
    return new_char
