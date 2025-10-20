#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download Bolivia electricity map layers (GeoNode @ geoportal.mhe.gob.bo) as GeoJSON,
and also produce a single merged GeoJSON. 

For 'geonode:transmision_sin_20250131', also export a flattened DataFrame as CSV
(transmision_sin_20250131.csv) preserving all original properties plus helper columns.

How to use in Sublime (or any IDE):
  1) Set OUTPUT_DIR below.
  2) Press Run. Files will be written into OUTPUT_DIR.


"""

import os, time, json, re, csv
from typing import Optional, Dict, List, Any
from collections import OrderedDict
import requests

import pandas as pd 

# ========= USER SETTINGS (edit these) =========
OUTPUT_DIR = "bolivia_electricity_map_2025"   # Folder will be created if missing
# Limit to specific layers, list them here (exact keys from LAYERS), e.g.:
# ONLY = ["geonode:transmision_sin_20250131", "geonode:gen_sin_20250131"]
ONLY: Optional[List[str]] = None
# ==============================================

WFS_URL = "https://geoportal.mhe.gob.bo/geoserver/ows"
SRS = "EPSG:4326"
PAGE_SIZE = 10000
TIMEOUT = 180

FILTER_ONLY_CODIGOS: list[str] = False #["CAR-SAD500"]#False  # e.g., ["CAR-SAD500"]

# Layer names confirmed from the public map/catalog.
LAYERS: Dict[str, str] = {
    "geonode:transmision_sin_20250131": "Líneas de Transmisión SIN 2025-01-31",
    "geonode:gen_sin_20250131":         "Centrales Generadoras SIN 2025-01-31",
    "geonode:Subestaciones_SIN_AGO_20230": "Subestaciones del SIN (2023-08)",
    #"geonode:Media_Ten_2024_2":         "Líneas de Media Tensión 2024",
    "geonode:Gen_Ais_2025":             "Centrales Generadoras (Sistemas Aislados) 2025",
}

# Optional: source pages to embed in output properties
LAYER_PAGES: Dict[str, str] = {
    "geonode:transmision_sin_20250131": "https://geoportal.mhe.gob.bo/layers/geonode%3Atransmision_sin_20250131",
    "geonode:gen_sin_20250131":         "https://geoportal.mhe.gob.bo/layers/geonode%3Agen_sin_20250131",
    "geonode:Subestaciones_SIN_AGO_20230": "https://geoportal.mhe.gob.bo/layers/geonode%3ASubestaciones_SIN_AGO_20230",
    #"geonode:Media_Ten_2024_2":         "https://geoportal.mhe.gob.bo/layers/geonode%3AMedia_Ten_2024_2",
    "geonode:Gen_Ais_2025":             "https://geoportal.mhe.gob.bo/layers/geonode%3AGen_Ais_2025",
}

session = requests.Session()
session.headers.update({"User-Agent": "BoliviaElectricityDownloader/1.3"})

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
        r = session.get(WFS_URL, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        m = re.search(r'numberMatched="(\d+)"', r.text)
        return int(m.group(1)) if m else None
    except Exception:
        return None

def fetch_wfs_geojson(layer: str, srs: str = SRS, page_size: int = PAGE_SIZE) -> dict:
    """
    Robust WFS fetch with progress prints.
    """
    print(f"  [STEP] Trying WFS 2.0.0 paginated fetch for {layer}")
    features = []
    start = 0
    attempts = 0
    while True:
        print(f"    [PAGE] Requesting features {start}–{start + page_size} ...")
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
        r = session.get(WFS_URL, params=params, timeout=TIMEOUT)
        print(f"    [HTTP] Status {r.status_code}")
        if r.status_code >= 400:
            print("    [WARN] WFS 2.0.0 failed, switching to fallback")
            break
        try:
            data = r.json()
        except Exception:
            print("    [ERROR] JSON decoding failed, switching to fallback")
            break
        page_feats = data.get("features") or []
        print(f"    [INFO] Retrieved {len(page_feats)} features this page")
        if not page_feats:
            break
        features.extend(page_feats)
        if len(page_feats) < page_size:
            break
        start += page_size
        attempts += 1
        if attempts > 200:
            print("    [STOP] Too many pages (>200), aborting")
            break
        time.sleep(0.25)

    if features:
        print(f"  [SUCCESS] Got {len(features)} total features via WFS 2.0.0")
        return {"type": "FeatureCollection", "features": features}

    print("  [FALLBACK] Trying WFS 1.0.0 one-shot")
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
        r = session.get(WFS_URL, params=params, timeout=TIMEOUT)
        print(f"  [HTTP] Status {r.status_code} (WFS 1.0.0)")
        r.raise_for_status()
        data = r.json()
        feats = data.get("features") or []
        if feats:
            print(f"  [SUCCESS] Got {len(feats)} features via WFS 1.0.0")
            return {"type": "FeatureCollection", "features": feats}
    except Exception as e:
        print(f"  [WARN] WFS 1.0.0 fallback failed: {e}")

    print("  [FALLBACK] Trying GeoNode layer_export ...")
    try:
        safe = layer.replace(':', '%3A')
        url = f"https://geoportal.mhe.gob.bo/layers/{safe}/layer_export?format=GeoJSON"
        print(f"  [INFO] GET {url}")
        r = session.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        feats = data.get("features") or []
        if feats:
            print(f"  [SUCCESS] Got {len(feats)} features via layer_export")
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
    """Reorder keys for readability: type, id, properties, geometry."""
    od = OrderedDict()
    od["type"] = f.get("type", "Feature")
    if "id" in f:
        od["id"] = f["id"]
    od["properties"] = f.get("properties", {})
    od["geometry"] = f.get("geometry", None)
    return od

def normalize_fc_readable(fc: dict) -> OrderedDict:
    """Ensure canonical FeatureCollection structure with stable key order."""
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
        f.write("\n")  # nice final newline
# ----------------------------------------

# ---------- DataFrame / CSV export for transmission lines ----------
TX_LAYER_KEY = "geonode:transmision_sin_20250131"

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
    for f in feats:
        props = dict(f.get("properties", {})) if f.get("properties") else {}
        # helper columns
        props["_feature_id"] = f.get("id")
        geom = f.get("geometry") or {}
        props["_geometry_type"] = geom.get("type")
        # compact coordinates representation (no whitespace) for reproducibility
        try:
            props["_coords_json"] = json.dumps(geom.get("coordinates", None), ensure_ascii=False, separators=(",", ":"))
        except Exception:
            props["_coords_json"] = None
        rows.append(props)
    return rows

def _write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("")
        return

    # Normalize Pn commas -> dots
    for r in rows:
        if "Pn" in r and isinstance(r["Pn"], str):
            r["Pn"] = r["Pn"].replace(",", ".")

    # Build DataFrame
    df = pd.DataFrame(rows)

    # Optional filter to one (or a few) Codigo
    if FILTER_ONLY_CODIGOS and "Codigo" in df.columns:
        df = df[df["Codigo"].astype(str).isin(FILTER_ONLY_CODIGOS)].copy()

    # Put helper columns first if present
    cols = list(df.columns)
    front = [c for c in ["_feature_id", "_geometry_type", "_coords_json"] if c in cols]
    df = df[front + [c for c in cols if c not in front]]

    # Force comma-separated CSV, UTF-8 BOM (Excel-friendly), with minimal quoting
    df.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
        sep=",",
        quoting=csv.QUOTE_MINIMAL,
    )
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
        count = None
        try:
            print(f"  [STEP] Checking numberMatched for {layer} ...")
            count = _number_matched(layer)
            if count is not None:
                print(f"  [INFO] numberMatched = {count}")
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
                print(f"  [STEP] Flattening transmission lines to CSV ...")
                csv_path = os.path.join(out_dir, "transmision_sin_20250131.csv")
                rows = _flatten_transmission_rows(fc)
                _write_csv(csv_path, rows)
                print(f"  [OK] CSV export done ({len(rows)} rows)")

        except Exception as e:
            failures += 1
            print(f"[ERROR] {layer}: {e}")

    merged_fc = {"type": "FeatureCollection", "features": merged}
    merged_path = os.path.join(out_dir, "bolivia_electricity_map_merged.geojson")
    print(f"[STEP] Writing merged GeoJSON ({len(merged)} features) ...")
    write_geojson_pretty(merged_path, merged_fc)
    print(f"[DONE] Merged file -> {merged_path}")
    print(f"[SUMMARY] Layers OK: {successes} | failed: {failures}")

if __name__ == "__main__":
    run()
