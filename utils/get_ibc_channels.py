#!/usr/bin/env python3
"""
Oneshot script to map IBC transfer channels for a set of LCD endpoints
and write a consolidated file `ibc-channels.json` at the repository root.

For each configured LCD in TENDERMINT_API_URLS:
  - Enumerate all `transfer` port channels (any state)
  - Resolve counterparty chain_id via connection -> client_state
  - Aggregate a nested mapping of the form:

    {
      "COINA": {
        "COINB": {
          "source channel": 56,
          "destiation channel": 6,
          "state": "OPEN"
        },
        ...
      },
      ...
    }

Notes:
  - Channel IDs are stored as integers where possible (e.g., channel-56 -> 56)
  - If a counterparty chain_id does not map to a known symbol, the raw
    chain_id is used as the nested key.
"""

import base64
import json
import sys
import time
from typing import Dict, List, Optional, Tuple
import os

import urllib.parse
import urllib.request
from logger import logger

TENDERMINT_API_URLS = {
    "IRISTEST": "https://iristest-api.bravo.komodo.earth/",
    "NUCLEUSTEST": "https://nucleus-api.alpha.komodo.earth/",
    "ATOM": "https://cosmos-api.alpha.komodo.earth/",
    "OSMO": "https://osmosis-api.alpha.komodo.earth/",
    "IRIS": "https://iris-rest.publicnode.com/"
}

def http_get_json(url: str, timeout: float = 20.0) -> Dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"Failed to decode JSON from {url}: {e}")


def urljoin(base: str, path: str, query: Optional[Dict[str, str]] = None) -> str:
    base = base.rstrip("/")
    path = path.lstrip("/")
    url = f"{base}/{path}"
    if query:
        url += "?" + urllib.parse.urlencode(query, doseq=True)
    return url


def paginate_channels(base: str, limit: int = 200) -> List[Dict]:
    """Fetch all channels using pagination.key."""
    path = "/ibc/core/channel/v1/channels"
    channels: List[Dict] = []
    next_key_b64: Optional[str] = None

    while True:
        q = {"pagination.limit": str(limit)}
        if next_key_b64:
            q["pagination.key"] = next_key_b64
        url = urljoin(base, path, q)
        data = http_get_json(url)
        cs = data.get("channels", [])
        channels.extend(cs)

        # Cosmos pagination.next_key is base64; empty means done
        pagination = data.get("pagination") or {}
        next_key_b64 = pagination.get("next_key")
        if not next_key_b64:
            break
    return channels


def get_channel(base: str, channel_id: str, port_id: str) -> Dict:
    path = f"/ibc/core/channel/v1/channels/{channel_id}/ports/{port_id}"
    url = urljoin(base, path)
    return http_get_json(url)


def get_connection(base: str, connection_id: str) -> Dict:
    path = f"/ibc/core/connection/v1/connections/{connection_id}"
    url = urljoin(base, path)
    return http_get_json(url)


def extract_chain_id_from_client_state(client_state_obj: Dict) -> Optional[str]:
    """
    client_state can be:
      {"client_state":{"@type":"...tendermint.ClientState","chain_id":"..." , ...}}
    OR packed as Any with 'value': {"chain_id": "..."}
    """
    cs = client_state_obj.get("client_state") or {}
    # direct field
    chain_id = cs.get("chain_id")
    if chain_id:
        return chain_id
    # packed google.protobuf.Any (some LCDs wrap as {"client_state": {"@type": "...", "value": {...}}})
    value = cs.get("value")
    if isinstance(value, dict):
        return value.get("chain_id")
    return None


def get_client_state(base: str, client_id: str) -> Dict:
    path = f"/ibc/core/client/v1/client_states/{client_id}"
    url = urljoin(base, path)
    return http_get_json(url)


def get_local_chain_id(base: str) -> Optional[str]:
    """Try to determine the chain_id of the local node.

    We attempt the Tendermint node_info endpoint first; if unavailable,
    we fall back to latest block header.
    """
    # Try node_info
    try:
        url = urljoin(base, "/cosmos/base/tendermint/v1beta1/node_info")
        data = http_get_json(url)
        # Newer LCDs use default_node_info.network; older may differ
        ni = data.get("default_node_info") or {}
        chain_id = ni.get("network")
        if chain_id:
            return chain_id
    except Exception:
        pass

    # Fallback: latest block
    try:
        url = urljoin(base, "/cosmos/base/tendermint/v1beta1/blocks/latest")
        data = http_get_json(url)
        header = (((data.get("block") or {}).get("header")) or {})
        chain_id = header.get("chain_id")
        if chain_id:
            return chain_id
    except Exception:
        pass
    return None


def map_channels(
    base: str,
    port_filter: str = "transfer",
    state_filter: Optional[str] = None,
    limit: int = 200,
    delay_ms: int = 0,
) -> List[Dict]:
    """
    Returns a list of dicts:
      {
        "channel_id": "channel-XX",
        "port_id": "transfer",
        "counterparty_channel_id": "channel-YY",
        "counterparty_port_id": "transfer",
        "connection_id": "connection-AB",
        "client_id": "07-tendermint-1234",
        "counterparty_chain_id": "cosmoshub-4"
      }
    """
    out: List[Dict] = []
    all_channels = paginate_channels(base, limit=limit)
    logger.info(f"Found {len(all_channels)} channels for {base}")
    for i, ch in enumerate(all_channels):
        logger.info(f"Processing channel {i+1} of {len(all_channels)} for {base}")
        try:
            port_id = ch.get("port_id")
            state = ch.get("state")
            if port_filter and port_id != port_filter:
                continue
            if state_filter and state != state_filter:
                continue

            channel_id = ch.get("channel_id")
            # Re-query channel for full detail (includes counterparty + connection_hops)
            ch_full = get_channel(base, channel_id, port_id)
            channel = ch_full.get("channel") or {}

            # Counterparty channel/port
            cp = channel.get("counterparty") or {}
            cp_channel_id = cp.get("channel_id")
            cp_port_id = cp.get("port_id")

            # First connection hop
            hops = channel.get("connection_hops") or []
            if not hops:
                # Some channels can be INIT/TRYOPEN without hops; skip if absent
                continue
            connection_id = hops[0]

            conn = get_connection(base, connection_id)
            connection = conn.get("connection") or {}
            client_id = connection.get("client_id")

            client_state_obj = get_client_state(base, client_id)
            chain_id = extract_chain_id_from_client_state(client_state_obj)

            info_dict = {
                "channel_id": channel_id,
                "port_id": port_id,
                "counterparty_channel_id": cp_channel_id,
                "counterparty_port_id": cp_port_id,
                "connection_id": connection_id,
                "client_id": client_id,
                "counterparty_chain_id": chain_id,
                "state": state,
            }
            out.append(info_dict)
            logger.info(f"Added channel {i+1} of {len(all_channels)} for {base}: {info_dict}")

            if delay_ms:
                time.sleep(delay_ms / 1000.0)

        except Exception as e:
            # Keep going; record a stub with the error for visibility
            out.append(
                {
                    "channel_id": ch.get("channel_id"),
                    "port_id": ch.get("port_id"),
                    "error": str(e),
                }
            )
    return out


def format_table(rows: List[Dict]) -> str:
    # Compute column widths
    headers = [
        "local_channel",
        "local_port",
        "counterparty_channel",
        "counterparty_port",
        "counterparty_chain_id",
        "connection_id",
        "client_id",
    ]
    table = []
    for r in rows:
        table.append(
            [
                r.get("channel_id", ""),
                r.get("port_id", ""),
                r.get("counterparty_channel_id", ""),
                r.get("counterparty_port_id", ""),
                r.get("counterparty_chain_id", ""),
                r.get("connection_id", ""),
                r.get("client_id", ""),
            ]
        )

    # widths
    widths = [max(len(str(x)) for x in [h] + [row[i] for row in table]) for i, h in enumerate(headers)]

    def fmt_row(row):
        return "  ".join(str(val).ljust(widths[i]) for i, val in enumerate(row))

    lines = [fmt_row(headers), fmt_row(["-" * w for w in widths])]
    for row in table:
        lines.append(fmt_row(row))
    return "\n".join(lines)


def parse_channel_number(channel_id: Optional[str]) -> Optional[int]:
    if not channel_id:
        return None
    try:
        if isinstance(channel_id, int):
            return channel_id
        if channel_id.startswith("channel-"):
            return int(channel_id.split("-", 1)[1])
        return int(channel_id)
    except Exception:
        return None


def normalize_state(state: Optional[str]) -> Optional[str]:
    if not state:
        return None
    # Convert e.g. STATE_OPEN -> OPEN
    if state.startswith("STATE_"):
        return state.split("_", 1)[1]
    return state


def build_chain_id_symbol_map() -> Dict[str, str]:
    """Discover a mapping from chain_id to our configured symbol labels."""
    mapping: Dict[str, str] = {}
    for symbol, base in TENDERMINT_API_URLS.items():
        base = base.rstrip("/")
        chain_id = get_local_chain_id(base)
        if chain_id:
            mapping[chain_id] = symbol
    return mapping


def aggregate_all() -> Dict[str, Dict[str, Dict[str, Optional[object]]]]:
    """Iterate over all configured endpoints and build the nested mapping."""
    chain_id_to_symbol = build_chain_id_symbol_map()

    result: Dict[str, Dict[str, Dict[str, Optional[object]]]] = {}

    for local_symbol, base in TENDERMINT_API_URLS.items():
        base = base.rstrip("/")
        try:
            logger.info(f"Fetching channels for {local_symbol} from {base}")
            rows = map_channels(base=base, port_filter="transfer", state_filter=None, limit=200, delay_ms=0)
            logger.info(f"Found {len(rows)} channels for {local_symbol}")
        except Exception as e:
            # Record an error stub for visibility and continue
            result.setdefault(local_symbol, {})["__error__"] = {"message": str(e)}
            continue

        local_map: Dict[str, Dict[str, Optional[object]]] = result.setdefault(local_symbol, {})

        # Prefer OPEN channels when multiple entries exist for the same counterparty
        def should_replace(existing: Optional[Dict[str, Optional[object]]], candidate_state: str, candidate_src_num: Optional[int]) -> bool:
            if not existing:
                return True
            existing_state = existing.get("state") or ""
            # Prefer OPEN over non-OPEN
            if existing_state != "OPEN" and candidate_state == "OPEN":
                return True
            if existing_state == "OPEN" and candidate_state != "OPEN":
                return False
            # Tie-breaker: choose smaller source channel number if both same state
            try:
                existing_num = int(existing.get("source channel")) if existing.get("source channel") is not None else None
            except Exception:
                existing_num = None
            if existing_num is None:
                return True
            if candidate_src_num is None:
                return False
            return candidate_src_num < existing_num

        for r in rows:
            if r.get("error"):
                # Skip errored rows from aggregation
                continue
            if r.get("port_id") != "transfer":
                continue

            cp_chain_id = r.get("counterparty_chain_id")
            cp_key = chain_id_to_symbol.get(cp_chain_id, cp_chain_id or "UNKNOWN")

            src_num = parse_channel_number(r.get("channel_id"))
            dst_num = parse_channel_number(r.get("counterparty_channel_id"))
            state_norm = normalize_state(r.get("state")) or "UNKNOWN"

            candidate = {
                "source channel": src_num,
                "destiation channel": dst_num,
                "state": state_norm,
            }
            existing = local_map.get(cp_key)
            if should_replace(existing, state_norm, src_num):
                local_map[cp_key] = candidate

    return result


def main():
    data = aggregate_all()

    # Write to repository root
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_path = os.path.join(repo_root, "utils", "ibc-channels.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)
    except Exception as e:
        print(f"ERROR writing {out_path}: {e}", file=sys.stderr)
        sys.exit(1)
    print(out_path)


if __name__ == "__main__":
    main()
