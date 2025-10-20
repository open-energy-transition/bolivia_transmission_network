#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download Bolivia electricity map layers (GeoNode @ geoportal.mhe.gob.bo) as GeoJSON,
and also produce a single merged GeoJSON.

For 'geonode:transmision_sin_20250131', also export a CSV preserving helper columns
and the original properties, with special comma→dot normalization for Pn and Sn.

This version preserves the original structure/flow but adds:
- Robust HTTP retries with exponential backoff (timeouts, 5xx/429, and "400 timeout-like")
- Safer JSON handling (avoid parsing HTML error pages)
- Clear, explicit prints of the exact SOURCE used (WFS 2.0.0 paginated / WFS 1.0.0 / layer_export)
- Windows-safe ASCII prints only (no special symbols)
- CSV writer with strict column order and Pn+Sn normalization
"""

import os, time, json, re, csv, sys
from typing import Optional, Dict, List, Any, Tuple
from collections import OrderedDict
import requests
import pandas as pd  # kept for compatibility; CSV writing below uses DictWriter

# --- Make Windows console tolerant to UTF-8 without crashing on prints ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ========= USER SETTINGS (edit these) =========
OUTPUT_DIR = "bolivia_electricity_map_2025"   # Folder will be created if missing
ONLY: Optional[List[str]] = None              # e.g., ["geonode:transmision_sin_20250131"]
# ==============================================

WFS_URL = "https://geoportal.mhe.gob.bo/geoserver/ows"
SRS = "EPSG:4326"
PAGE_SIZE = 10000

# Resilience knobs (defaults mirror the patched variant)
MAX_RETRIES = 6
TIMEOUT_CONNECT = 8
TIMEOUT_READ = 90
BACKOFF_BASE = 1.6

FILTER_ONLY_CODIGOS: Optional[List[str]] = None  # e.g., ["CAR-SAD500"]

# Layer names confirmed from the public map/catalog.
LAYERS: Dict[str, str] = {
    "geonode:transmision_sin_20250131":     "Líneas de Transmisión SIN 2025-01-31",
    "geonode:gen_sin_20250131":             "Centrales Generadoras SIN 2025-01-31",
    "geonode:Subestaciones_SIN_AGO_20230":  "Subestaciones del SIN (2023-08)",
    "geonode:Gen_Ais_2025":                 "Centrales Generadoras (Sistemas Aislados) 2025",
}

# Optional: source pages to embed in output properties
LAYER_PAGES: Dict[str, str] = {
    "geonode:transmision_sin_20250131":     "https://geoportal.mhe.gob.bo/layers/geonode%3Atransmision_sin_20250131",
    "geonode:gen_sin_20250131":             "https://geoportal.mhe.gob.bo/layers/geonode%3Agen_sin_20250131",
    "geonode:Subestaciones_SIN_AGO_20230":  "https://geoportal.mhe.gob.bo/layers/geonode%3ASubestaciones_SIN_AGO_20230",
    "geonode:Gen_Ais_2025":                 "https://geoportal.mhe.gob.bo/layers/geonode%3AGen_Ais_2025",
}

session = requests.Session()
session.headers.update({"User-Agent": "BoliviaElectricityDownloader/1.4-resilient"})

# ---------------- Robust HTTP helpers ----------------
def _is_timeout_like_400(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return ("timeout" in t) or ("time out" in t) or ("timed out" in t)

def get_with_retries(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = MAX_RETRIES,
    tconn: int = TIMEOUT_CONNECT,
    tread: int = TIMEOUT_READ,
    backoff_base: float = BACKOFF_BASE,
) -> requests.Response:
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = session.get(url, params=params, headers=headers, timeout=(tconn, tread))
            ctype = resp.headers.get("Content-Type", "").lower()
            text_peek = resp.text[:4000] if ("text/" in ctype or "html" in ctype) else ""

            # Retry on server overload/temporary errors
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Retryable status {resp.status_code}")

            # Some servers return 400 for internal timeouts
            if resp.status_code == 400 and _is_timeout_like_400(text_peek):
                raise requests.HTTPError("400 with timeout-like body")

            if resp.status_code != 200:
                resp.raise_for_status()

            # Treat 200 HTML error pages as errors
            if "<html" in text_peek and "error" in text_peek:
                raise requests.HTTPError("HTML error page with 200 status")

            return resp

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, requests.RequestException) as e:
            if attempt > max_retries:
                raise
            sleep_s = backoff_base ** (attempt - 1) + (0.1 * attempt)
            print(f"  [retry {attempt}/{max_retries}] {url} -> {type(e).__name__}: {e}. Sleeping {sleep_s:.1f}s...", flush=True)
            time.sleep(sleep_s)

# ---------------- Core helpers (same style as before) ----------------
def _number_matched(layer: str) -> Optional[int]:
    """Try WFS 2.0.0 hits request to get total feature count (informational)."""
    try:
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": layer,
            "resultType": "hits",
        }
        r = get_with_retries(WFS_URL, params=params)
        m = re.search(r'numberMatched="(\d+)"', r.text)
        return int(m.group(1)) if m else None
    except Exception:
        return None

def fetch_wfs_geojson(layer: str, srs: str = SRS, page_size: int = PAGE_SIZE) -> dict:
    """
    Robust WFS fetch with progress prints and explicit SOURCE reporting.
    """
    print(f"  [STEP] Trying WFS 2.0.0 paginated fetch for {layer}")
    features = []
    start = 0
    attempts = 0
    while True:
        print(f"    [PAGE] Requesting features {start}..{start + page_size} via WFS 2.0.0")
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": layer,
            "outputFormat": "application/json",
            "srsName": srs,
            "count": page_size,
            "startIndex": start,
        }
        try:
            r = get_with_retries(WFS_URL, params=params)
        except Exception as e:
            print(f"    [WARN] WFS 2.0.0 page failed: {e}. Falling back.")
            break

        ctype = r.headers.get("Content-Type", "").lower()
        if "json" not in ctype:
            text_head = r.text[:200].replace("\n", " ")
            print(f"    [WARN] Non-JSON content-type: {ctype}; head: {text_head}. Falling back.")
            break

        try:
            data = r.json()
        except Exception as e:
            print(f"    [ERROR] JSON decoding failed: {e}. Falling back.")
            break

        page_feats = data.get("features") or []
        print(f"    [INFO] Retrieved {len(page_feats)} features this page")
        if not page_feats:
            break
        features.extend(page_feats)
        if len(page_feats) < page_size:
            print("  [SOURCE] USED: WFS 2.0.0 paginated")
            return {"type": "FeatureCollection", "features": features}
        start += page_size
        attempts += 1
        if attempts > 200:
            print("    [STOP] Too many pages (>200), aborting WFS 2.0.0")
            break
        time.sleep(0.25)

    print("  [FALLBACK] Trying WFS 1.0.0 one-shot ...")
    try:
        params = {
            "service": "WFS",
            "version": "1.0.0",
            "request": "GetFeature",
            "typeName": layer,
            "outputFormat": "application/json",
            "srsName": srs,
            "maxFeatures": 200000,
        }
        r = get_with_retries(WFS_URL, params=params)
        data = r.json()
        feats = data.get("features") or []
        if feats:
            print("  [SOURCE] USED: WFS 1.0.0 one-shot")
            return {"type": "FeatureCollection", "features": feats}
    except Exception as e:
        print(f"  [WARN] WFS 1.0.0 fallback failed: {e}")

    print("  [FALLBACK] Trying GeoNode layer_export (GeoJSON) ...")
    try:
        safe = layer.replace(':', '%3A')
        url = f"https://geoportal.mhe.gob.bo/layers/{safe}/layer_export?format=GeoJSON"
        print(f"  [INFO] GET {url}")
        r = get_with_retries(url)
        data = r.json()
        feats = data.get("features") or []
        if feats:
            print("  [SOURCE] USED: GeoNode layer_export")
            return {"type": "FeatureCollection", "features": feats}
    except Exception as e:
        print(f"  [ERROR] layer_export fallback failed: {e}")

    raise RuntimeError(f"Could not fetch layer via any method: {layer}")

def add_metadata(fc: dict, layer_name: str, title: str) -> dict:
    src_url = LAYER_PAGES.get(layer_name, f"https://geoportal.mhe.gob.bo/layers/{layer_name.replace(':', '%3A')}")
    for f in fc.get("features", []):
        props = f.setdefault("properties", {})
        props.setdefault("_source_layer", layer_name)
        props.setdefault("_layer_title", title)
        props.setdefault("_source_url", src_url)
        props.setdefault("_license_hint",
            "Check layer page for license; attribute Ministerio de Hidrocarburos y Energías"
        )
    return fc

def sanitize_filename(name: str) -> str:
    return name.replace(":", "_").replace("/", "_")

# ---------- Readability helpers ----------
def _ordered_feature(f: dict) -> OrderedDict:
    od = OrderedDict()
    od["type"] = f.get("type", "Feature")
    if "id" in f:
        od["id"] = f["id"]
    od["properties"] = f.get("properties", {})
    od["geometry"] = f.get("geometry", None)
    return od

def normalize_fc_readable(fc: dict) -> OrderedDict:
    feats = fc.get("features", [])
    feats_ordered = [_ordered_feature(f) for f in feats]
    out = OrderedDict()
    out["type"] = "FeatureCollection"
    out["features"] = feats_ordered
    return out

def write_geojson_pretty(path: str, fc: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    readable = normalize_fc_readable(fc)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(readable, f, ensure_ascii=False, indent=2)
        f.write("\n")
# ----------------------------------------

# ---------- DataFrame / CSV export for transmission lines ----------
TX_LAYER_KEY = "geonode:transmision_sin_20250131"

# Strict column order requested (helpers first), like you asked:
CSV_COLS = [
    "_feature_id","_geometry_type","_coords_json",
    "fid","OBJECTID","Codigo","AREA","PROPIETARI","STI","TRAMO","TIPO","N_CIRCU",
    "Un","Long","Pn","NODO_1","INTERR_1","NODO_2","INTERR_2","In_","Sbase",
    "R1","X1","C1","R0","X0","C0","Zc","Sn","SOBCAR15","SOBCAR30","R11","X11","Qvacio",
    "_source_layer","_layer_title","_source_url","_license_hint",
]

def _flatten_transmission_rows(fc: dict) -> List[Dict[str, Any]]:
    """
    Flatten FeatureCollection -> list of rows.
    Keeps all original properties; adds helper columns:
      - _feature_id
      - _geometry_type
      - _coords_json  (compact coordinates; GeoJSON array as string)
    """
    rows: List[Dict[str, Any]] = []
    feats = fc.get("features", [])
    for i, f in enumerate(feats, start=1):
        props = dict(f.get("properties", {})) if f.get("properties") else {}
        # helper columns
        props["_feature_id"] = f.get("id", i)
        geom = f.get("geometry") or {}
        props["_geometry_type"] = geom.get("type", "")
        # compact coordinates representation (no whitespace) for reproducibility
        try:
            props["_coords_json"] = json.dumps(geom.get("coordinates", None), ensure_ascii=False, separators=(",", ":"))
        except Exception:
            props["_coords_json"] = ""
        rows.append(props)
    return rows

def _write_csv_strict(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            f.write("")
        return

    # Special numeric normalization: only Pn and Sn (comma → dot)
    for r in rows:
        for k in ("Pn", "Sn"):
            if k in r and isinstance(r[k], str):
                r[k] = r[k].replace(",", ".")

    # Ensure all required columns exist
    for r in rows:
        for c in CSV_COLS:
            if c not in r or r[c] is None:
                r[c] = ""

    # Write strictly in the requested order, UTF-8 BOM, minimal quoting
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in CSV_COLS})

# -------------------------------------------------------------------

def run():
    out_dir = OUTPUT_DIR
    os.makedirs(out_dir, exist_ok=True)
    print(f"[START] Output directory: {os.path.abspath(out_dir)}")

    layer_list = list(LAYERS.items())
    if ONLY:
        only_set = set(ONLY)
        layer_list = [(k, v) for k, v in layer_list if k in only_set]
        print(f"[INFO] Limiting to layers: {', '.join(only_set)}")

    merged = []
    successes = 0
    failures = 0

    for i, (layer, title) in enumerate(layer_list, 1):
        print(f"\n[==== {i}/{len(layer_list)} Processing {layer} ====]")
        try:
            print(f"  [STEP] Checking numberMatched for {layer} ...")
            count = _number_matched(layer)
            if count is not None:
                print(f"  [INFO] numberMatched ~= {count}")
            else:
                print("  [WARN] Could not determine numberMatched")

            print(f"  [STEP] Fetching data for {layer} ...")
            fc = fetch_wfs_geojson(layer)
            print(f"  [STEP] Adding metadata for {layer}")
            fc = add_metadata(fc, layer, title)

            out_path = os.path.join(out_dir, f"{sanitize_filename(layer)}.geojson")
            print(f"  [STEP] Writing GeoJSON to {out_path}")
            write_geojson_pretty(out_path, fc)
            print(f"  [OK] {layer}: {len(fc['features'])} features saved")
            merged.extend(fc["features"])
            successes += 1

            if layer == TX_LAYER_KEY:
                print(f"  [STEP] Flattening transmission lines to CSV (strict columns) ...")
                csv_path = os.path.join(out_dir, "transmision_sin_20250131.csv")
                rows = _flatten_transmission_rows(fc)

                # Optional filter to specific Codigo(s)
                if FILTER_ONLY_CODIGOS and rows:
                    rows = [r for r in rows if str(r.get("Codigo", "")) in FILTER_ONLY_CODIGOS]

                _write_csv_strict(csv_path, rows)
                print(f"  [OK] CSV export done ({len(rows)} rows)")

        except Exception as e:
            failures += 1
            print(f"[ERROR] {layer}: {e}")

    merged_fc = {"type": "FeatureCollection", "features": merged}
    merged_path = os.path.join(out_dir, "bolivia_electricity_map_merged.geojson")
    print(f"[STEP] Writing merged GeoJSON ({len(merged)} features) -> {merged_path}")
    write_geojson_pretty(merged_path, merged_fc)
    print(f"[SUMMARY] Layers OK: {successes} | failed: {failures}")
    print("[DONE]")

if __name__ == "__main__":
    run()
