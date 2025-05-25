import requests
from typing import List, Dict
import time

API_CHAR      = "https://api.vndb.org/kana/character"
API_VN        = "https://api.vndb.org/kana/vn"
HEADERS       = {
    "Content-Type": "application/json",
}
# Fields we want from each character
CHAR_FIELDS   = [
        "id", 
        "birthday",
        "sex",
        "age",
        "name", 
        "image.url",
        "traits.name",
        "traits.group_name",
        "vns.role", 
        "vns.developers.name", 
        "vns.released",
    ]
CHAR_FIELDS_PARAM  = ",".join(CHAR_FIELDS)

def fetch_characters_by_vn_id(vn_id: str) -> List[Dict]:
    """
    Fetches all characters for the given VN ID via VNDB REST API.
    Paginates through results until 'more' == False.

    :param vn_id: The VNDB ID string (e.g. "v95" or just "95").
    :return: List of character dicts containing the requested fields.
    """
    url = API_CHAR
    characters: List[Dict] = []
    page = 1

    # Ensure vn_id has the 'v' prefix
    if not vn_id.startswith("v"):
        vn_id = f"v{vn_id}"

    while True:
        payload = {
            "filters": ["and", 
                            ["vn", "=", ["id", "=", vn_id]],
                            ["or", 
                                ["role", "=", "main"],
                                ["role", "=", "primary"],
                            ],
                        ],
            "fields": CHAR_FIELDS_PARAM,
            "page": page,
        }

        resp = requests.post(url, headers=HEADERS, json=payload)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("results", [])
        characters.extend(batch)

        # Stop when no more pages
        if not data.get("more", False):
            break

        page += 1

    return characters

def fetch_top_vns(limit: int = 1, page: int = 1, sort: str = "votecount") -> List[Dict]:
    """
    Fetches the top VN entries from VNDB.

    :param limit: Number of pages to fetch (default is 1).
    :param page: Page number to start fetching from (default is 1).
    :return: List of VN dicts.
    """
    url = API_VN
    if limit < 1:
        raise ValueError("Limit must be at least 1")
    vn_count = 0
    vns: List[Dict] = []
    while page <= limit:
        payload = {
            "filters": [],
            "fields": "id,title",
            "page": page,
            "reverse": True,
            "sort": sort,
        }

        resp = requests.post(url, headers=HEADERS, json=payload)
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("results", [])
        

        # Stop when no more pages
        if not data.get("more", False):
            break

        page += 1
        vns.extend(batch)
        vn_count += len(batch)

    return vns

def fetch_vn_name_by_id(vn_id: str) -> Dict:
    """
    Fetches the name of a VN by its ID.

    :param vn_id: The VNDB ID string (e.g. "v95" or just "95").
    :return: The title of the VN.
    """
    url = API_VN
    
    payload = {
        "filters": ["id", "=", vn_id],
        "fields": "id,title",
    }

    resp = requests.post(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    data = resp.json()

    return data.get("results", [{}])[0] 