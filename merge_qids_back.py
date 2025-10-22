#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge Wikidata QIDs back into:
  - transmision_sin_20250131_with_qid.csv (adds 'wikidata' right after 'Codigo')
  - geonode_transmision_sin_20250131_with_qid.geojson (brand-new GeoJSON built from merged CSV)

Process:
1) Read transmission CSV from OUTPUT_DIR.
2) Compute EXT token per row: sha1(_feature_id + "|" + first 256 chars of _coords_json)[:12].
3) Query Wikidata SPARQL by Codigo (wdt:P528) in batches (VALUES), throttled + retries.
4) If multiple items share a Codigo, prefer the one whose Spanish description contains "[EXT:<token>]".
5) Write CSV (UTF-8 BOM) with 'wikidata' inserted AFTER 'Codigo' and Excel-safe '_coords_json'.
6) Build a NEW GeoJSON from the merged DataFrame using *_geometry_type* + original *_coords_json*.
"""

import os, json, time, hashlib, sys
from typing import List, Dict, Any, Optional
import requests
import pandas as pd

# ---------------- CONFIG ----------------
OUTPUT_DIR = "bolivia_electricity_map_2025"

INPUT_CSV  = os.path.join(OUTPUT_DIR, "transmision_sin_20250131.csv")
OUT_CSV    = os.path.join(OUTPUT_DIR, "transmision_sin_20250131_with_qid.csv")
OUT_GEOJSON = os.path.join(OUTPUT_DIR, "geonode_transmision_sin_20250131_with_qid.geojson")

SPARQL_URL = "https://query.wikidata.org/sparql"
USER_AGENT = "BoliviaElectricityMap/merge-qids (mailto:your-email@example.org)"

# Batching & etiquette
BATCH_SIZE     = 100   # 50–150 is a good range for WDQS; tune as needed
THROTTLE_SECS  = 2.8   # keep ≥ ~2.5–3.0s between batch requests
RETRIES        = 5
BACKOFF        = 1.6
LANG_DESC      = "es"  # prefer Spanish descriptions for EXT disambiguation

# Windows console: avoid Unicode crashes on prints
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# --------------- Utilities ---------------
def ext_token(fid: str, coords_json: str) -> str:
    """sha1(fid + '|' + first 256 chars of coords_json)[:12]."""
    basis = f"{fid}|{(coords_json or '')[:256]}"
    return hashlib.sha1(basis.encode("utf-8", "ignore")).hexdigest()[:12]

def qid_from_uri(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]

def chunked(seq: List[str], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def http_post_sparql(query: str) -> Dict[str, Any]:
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": USER_AGENT,
    }
    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.post(SPARQL_URL, data={"query": query}, headers=headers, timeout=90)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Retryable {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt >= RETRIES:
                raise
            sleep_s = (BACKOFF ** (attempt - 1)) + 0.1 * attempt
            print(f"[retry {attempt}/{RETRIES}] SPARQL -> {e}. Sleeping {sleep_s:.1f}s", flush=True)
            time.sleep(sleep_s)

def build_sparql_for_codigos(codes: List[str]) -> str:
    """
    Return items with P528 == any of the given codes and their es-description.
    """
    # Escape any double quotes and build VALUES
    vals = " ".join(f'"{c.replace(chr(34), "")}"' for c in codes if c)
    return f"""
SELECT ?item ?code ?desc WHERE {{
  VALUES ?code {{ {vals} }}
  ?item wdt:P528 ?code .
  OPTIONAL {{
    ?item schema:description ?desc .
    FILTER (LANG(?desc) = "{LANG_DESC}")
  }}
}}
"""

# --------------- GeoJSON builder ---------------
def dataframe_to_geojson(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Build a fresh GeoJSON FeatureCollection from the merged DataFrame.
    - Uses _geometry_type + ORIGINAL _coords_json for geometry.
    - Includes all row properties (including 'wikidata').
    - If geometry cannot be parsed, the feature is skipped.
    """
    features = []
    skipped = 0
    # We kept a copy of original coords in '_coords_json_orig'
    for idx, row in df.iterrows():
        gtype = str(row.get("_geometry_type", "") or "").strip()
        coords_src = row.get("_coords_json_orig", "")
        if not gtype or not coords_src:
            skipped += 1
            continue
        try:
            coords = json.loads(coords_src)
        except Exception:
            skipped += 1
            continue

        # Properties: everything from the row, but do not include the '_coords_json_orig' helper
        props = row.to_dict()
        props.pop("_coords_json_orig", None)
        # Keep the Excel-safe _coords_json in properties as-is (it's fine; geometry is authoritative)

        # Feature id: use _feature_id if available, else the row index
        fid = props.get("_feature_id", None)
        feat = {
            "type": "Feature",
            "id": fid if fid not in (None, "", "nan") else idx + 1,
            "properties": props,
            "geometry": {"type": gtype, "coordinates": coords},
        }
        features.append(feat)

    print(f"[INFO] GeoJSON features built: {len(features)} | skipped (bad/missing geometry): {skipped}")
    return {"type": "FeatureCollection", "features": features}

# --------------- Main ---------------
def main():
    # 1) Load CSV
    if not os.path.isfile(INPUT_CSV):
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV, dtype=str, encoding="utf-8", low_memory=False)

    required = ["Codigo", "_feature_id", "_coords_json", "_geometry_type"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {INPUT_CSV}: {missing}")

    # Normalize strings & compute EXT (compute BEFORE any Excel-safe transforms)
    df["Codigo"] = df["Codigo"].fillna("").astype(str).str.strip()
    df["_feature_id"] = df["_feature_id"].fillna("").astype(str)
    df["_coords_json"] = df["_coords_json"].fillna("").astype(str)
    df["_geometry_type"] = df["_geometry_type"].fillna("").astype(str)

    # Keep an ORIGINAL copy of coords for hashing & GeoJSON geometry rebuild
    df["_coords_json_orig"] = df["_coords_json"]

    df["_ext_token"] = [ext_token(fid, cj) for fid, cj in zip(df["_feature_id"], df["_coords_json"])]

    # 2) Query Wikidata by Codigo (P528) in batches
    unique_codes = sorted({c for c in df["Codigo"].tolist() if c})
    print(f"[INFO] Unique Codigo values to resolve: {len(unique_codes)}")

    code_to_hits: Dict[str, List[Dict[str, str]]] = {}
    total_candidates = 0

    for batch in chunked(unique_codes, BATCH_SIZE):
        batch_start = time.time()

        q = build_sparql_for_codigos(batch)
        data = http_post_sparql(q)

        for b in data.get("results", {}).get("bindings", []):
            code = b.get("code", {}).get("value", "")
            item = b.get("item", {}).get("value", "")
            desc = b.get("desc", {}).get("value", "")
            if code and item:
                code_to_hits.setdefault(code, []).append({
                    "qid": qid_from_uri(item),
                    "desc": desc or ""
                })
                total_candidates += 1

        print(f"[INFO] batch {len(batch)} codes -> {total_candidates} candidates total")

        # Throttle between batch requests to be nice to WDQS
        elapsed = time.time() - batch_start
        if elapsed < THROTTLE_SECS:
            time.sleep(THROTTLE_SECS - elapsed)

    # 3) Decide best QID per row (disambiguate with [EXT:<token>] when needed)
    chosen_qids: List[Optional[str]] = []
    unresolved = ambiguous = 0

    for _, row in df.iterrows():
        code = row["Codigo"]
        token = row["_ext_token"]
        hits = code_to_hits.get(code, [])

        if not hits:
            chosen_qids.append(None)
            unresolved += 1
            continue

        if len(hits) == 1:
            chosen_qids.append(hits[0]["qid"])
            continue

        tag = f"[EXT:{token}]"
        with_ext = [h for h in hits if tag in h["desc"]]
        if len(with_ext) == 1:
            chosen_qids.append(with_ext[0]["qid"])
        else:
            chosen_qids.append(None)
            ambiguous += 1

    df["wikidata"] = [q if isinstance(q, str) else "" for q in chosen_qids]
    print(f"[SUMMARY] rows={len(df)} | with_qid={df['wikidata'].astype(bool).sum()} | unresolved={unresolved} | ambiguous={ambiguous}")

    # 4) Make _coords_json Excel-safe (AFTER hashing and before writing CSV)
    if "_coords_json" in df.columns:
        df["_coords_json"] = (
            df["_coords_json"]
            .astype(str)
            .str.replace("\n", "\\n", regex=False)
            .str.replace("\r", "",    regex=False)
            .str.replace(",", "‚",    regex=False)   # replace comma with low-9 comma
        )

    # 5) Write CSV with 'wikidata' INSERTED RIGHT AFTER 'Codigo' (UTF-8 BOM)
    cols = list(df.columns)
    # Move 'wikidata' right after 'Codigo'
    if "wikidata" in cols:
        cols.remove("wikidata")
    if "Codigo" not in cols:
        raise ValueError("'Codigo' column is required to position 'wikidata'.")
    insert_at = cols.index("Codigo") + 1
    cols.insert(insert_at, "wikidata")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df[cols].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"[OK] CSV written -> {OUT_CSV}")

    # 6) Build a NEW GeoJSON from the merged DataFrame (using ORIGINAL coords)
    fc = dataframe_to_geojson(df)
    with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"[OK] GeoJSON written -> {OUT_GEOJSON}")

if __name__ == "__main__":
    main()
