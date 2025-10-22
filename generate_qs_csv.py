#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, hashlib
from decimal import Decimal, InvalidOperation
import pandas as pd

# ---------- PATH CONFIG ----------
OUTPUT_DIR = "bolivia_electricity_map_2025"
INPUT_CSV  = os.path.join(OUTPUT_DIR, "transmision_sin_20250131.csv")
QS_OUT     = os.path.join(OUTPUT_DIR, "qs_transmision_upload.csv")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------- CONSTANTS ----------
S248_QID   = "Q136465248"
S854_URL   = '"https://geoportal.mhe.gob.bo/layers/geonode:transmision_sin_20250131"'
S813_TIME  = "+2025-10-16T00:00:00Z/11"

Q_OVERHEAD_LINE = "Q2144320"   # P31 (instance of: overhead power line)
Q_BOLIVIA       = "Q750"       # P17 (country: Bolivia)
U_METRE         = "U828224"    # unit for P2043 (metre)
Q_VOLT          = "Q25250"     # volt

Q_RESISTANCE     = "Q25358"
Q_REACTANCE      = "Q193972"
Q_CAPACITANCE    = "Q164399"
Q_IMPEDANCE      = "Q179043"
Q_APPARENT_POWER = "Q1930258"

Q_POS_SEQ  = "Q136510769"
Q_ZERO_SEQ = "Q136510773"

# ---------- COLUMN NAMES ----------
COL_QID     = "qid"
COL_CODE    = "Codigo"
COL_TRAMO   = "TRAMO"
COL_VOLTAGE = "Un"
COL_LENGTH  = "Long"
COL_R1      = "R1"
COL_X1      = "X1"
COL_C1      = "C1"
COL_R0      = "R0"
COL_X0      = "X0"
COL_C0      = "C0"
COL_ZC      = "Zc"
COL_SN      = "Sn"
COL_FID     = "_feature_id"
COL_COORDSJ = "_coords_json"

# ---------- HELPERS ----------
def plain_decimal(v) -> str:
    """Return a plain (non-scientific) decimal string or empty string."""
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    try:
        d = Decimal(s)
        return format(d, "f")
    except (InvalidOperation, ValueError):
        return s

def ext_token(fid: str, coords_json: str) -> str:
    basis = f"{fid}|{(coords_json or '')[:256]}"
    return hashlib.sha1(basis.encode("utf-8", "ignore")).hexdigest()[:12]

def coord_from_multilinestring(coords_json: str) -> str:
    """
    Compute centroid of a GeoJSON MultiLineString (average of all vertices).
    Returns '@lat/lon' for QuickStatements P625.
    """
    try:
        geom = json.loads(coords_json)  # expect [[[lon, lat], ...], ...]
        all_points = [(lon, lat) for line in geom for lon, lat in line]
        if not all_points:
            return ""
        lon_mean = sum(p[0] for p in all_points) / len(all_points)
        lat_mean = sum(p[1] for p in all_points) / len(all_points)
        return f"@{lat_mean}/{lon_mean}"
    except Exception:
        return ""

# ---------- BUILD ONE QS ROW ----------
def build_row(rec: dict) -> list:
    out = []
    # qid, labels (language-specific), descriptions (language-specific)
    out += [rec["qid"], rec["Len"], rec["Les"], rec["Den"], rec["Des"]]

    # P31, P17, P625, P528, P2436 (voltage in volts with unit), P2043 (length in metres)
    out += [Q_OVERHEAD_LINE, S248_QID, S854_URL, S813_TIME]
    out += [Q_BOLIVIA,      S248_QID, S854_URL, S813_TIME]
    out += [rec["P625"],    S248_QID, S854_URL, S813_TIME]
    out += [f'"{rec["P528"]}"', S248_QID, S854_URL, S813_TIME]
    out += [rec["P2436"],   S248_QID, S854_URL, S813_TIME]
    out += [f'+{rec["P2043"]}{U_METRE}' if rec["P2043"] else "", S248_QID, S854_URL, S813_TIME]

    # P1114 blocks with qualifiers + per-statement refs
    def add_qty(val, q_char, p518=None):
        nonlocal out
        # keep column count consistent even if value is missing
        if not val:
            if p518 is None:
                out.extend(["", "", S248_QID, S854_URL, S813_TIME])
            else:
                out.extend(["", "", "", S248_QID, S854_URL, S813_TIME])
            return
        if p518 is None:
            out.extend([val, q_char, S248_QID, S854_URL, S813_TIME])
        else:
            out.extend([val, q_char, p518, S248_QID, S854_URL, S813_TIME])

    # R1, X1, C1 (positive sequence)
    add_qty(rec["R1"], Q_RESISTANCE,  Q_POS_SEQ)
    add_qty(rec["X1"], Q_REACTANCE,   Q_POS_SEQ)
    add_qty(rec["C1"], Q_CAPACITANCE, Q_POS_SEQ)
    # R0, X0, C0 (zero sequence)
    add_qty(rec["R0"], Q_RESISTANCE,  Q_ZERO_SEQ)
    add_qty(rec["X0"], Q_REACTANCE,   Q_ZERO_SEQ)
    add_qty(rec["C0"], Q_CAPACITANCE, Q_ZERO_SEQ)
    # Zc (no P518) and Sn (no P518)
    add_qty(rec["Zc"], Q_IMPEDANCE, None)
    add_qty(rec["Sn"], Q_APPARENT_POWER, None)

    return out

# ---------- MAIN ----------
def main():
    df = pd.read_csv(
        INPUT_CSV,
        encoding="utf-8",
        sep=",",
        quotechar='"',
        doublequote=True,
        escapechar="\\",
        engine="python"
    )

    rows = []
    for _, r in df.iterrows():
        qid = r.get(COL_QID)
        # For CSV QuickStatements: leave qid BLANK to create new items
        qid_cell = qid if (isinstance(qid, str) and str(qid).startswith("Q")) else ""

        code  = str(r.get(COL_CODE, "")).strip()
        tramo = str(r.get(COL_TRAMO, "")).strip()
        coords_json = str(r.get(COL_COORDSJ, "")).strip()
        coords = coord_from_multilinestring(coords_json)
        token = ext_token(str(r.get(COL_FID, "")), coords_json)

        # Voltage → volts (×1000) with explicit unit U25250
        try:
            voltage_value = Decimal(str(r.get(COL_VOLTAGE))) * 1000
            voltage_str = f"{format(voltage_value, 'f')}U{Q_VOLT[1:]}"  # e.g., 500000U25250
        except Exception:
            voltage_str = ""

        label = code or "Linea/circuito del SIN"
        desc_text_es = (tramo or "Linea/circuito del SIN de Bolivia") + f" [EXT:{token}]"
        # If you prefer different English text, tweak here:
        desc_text_en = desc_text_es

        rec = {
            "qid": qid_cell,
            # Labels
            "Len": label,   # English label
            "Les": label,   # Spanish label
            # Descriptions
            "Den": desc_text_en,
            "Des": desc_text_es,
            # Statements
            "P625": coords,
            "P528": label,
            "P2436": voltage_str,                         # voltage in V with unit
            "P2043": plain_decimal(r.get(COL_LENGTH)),    # length (metres unit added later)
            "R1": plain_decimal(r.get(COL_R1)),
            "X1": plain_decimal(r.get(COL_X1)),
            "C1": plain_decimal(r.get(COL_C1)),
            "R0": plain_decimal(r.get(COL_R0)),
            "X0": plain_decimal(r.get(COL_X0)),
            "C0": plain_decimal(r.get(COL_C0)),
            "Zc": plain_decimal(r.get(COL_ZC)),
            "Sn": plain_decimal(r.get(COL_SN)),
        }
        rows.append(build_row(rec))

    header = [
        # qid + labels + descriptions (no plain L)
        "qid","Len","Les","Den","Des",
        # P31 (instance of) + refs
        "P31","S248","s854","s813",
        # P17 (country) + refs
        "P17","S248","s854","s813",
        # P625 (coordinates) + refs
        "P625","S248","s854","s813",
        # P528 (identifier/code) + refs
        "P528","S248","s854","s813",
        # P2436 (voltage) + refs
        "P2436","S248","s854","s813",
        # P2043 (length) + refs
        "P2043","S248","s854","s813",
        # Quantities P1114 with qualifiers + refs
        "P1114","qal13044","qal518","S248","s854","s813",  # R1 (+seq)
        "P1114","qal13044","qal518","S248","s854","s813",  # X1 (+seq)
        "P1114","qal13044","qal518","S248","s854","s813",  # C1 (+seq)
        "P1114","qal13044","qal518","S248","s854","s813",  # R0 (0-seq)
        "P1114","qal13044","qal518","S248","s854","s813",  # X0 (0-seq)
        "P1114","qal13044","qal518","S248","s854","s813",  # C0 (0-seq)
        "P1114","qal13044","S248","s854","s813",           # Zc (no P518)
        "P1114","qal13044","S248","s854","s813"            # Sn (no P518)
    ]

    pd.DataFrame(rows, columns=header).to_csv(QS_OUT, index=False, encoding="utf-8-sig")
    print(f"[OK] File created: {QS_OUT} ({len(rows)} rows)")

if __name__ == "__main__":
    main()
