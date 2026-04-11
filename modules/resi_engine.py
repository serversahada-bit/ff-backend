# engine.py
import io, re
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Tuple, Optional
from urllib.parse import parse_qs, urlparse

import fitz  # PyMuPDF
import pandas as pd
import requests
from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader

# ================= CONFIG =================
MASTER_URL_DEFAULT = "https://docs.google.com/spreadsheets/d/1ILxRMeDewtfLGckkPcY2CHHXiBtKK4Gc__lTNUZxGis/edit?gid=727982747#gid=727982747"

A6_W_MM, A6_H_MM = 105, 148

# >>> TUNING TAMPILAN SESUAI GAMBAR
MARGIN_MM = 3                 # Margin pinggir kertas
BOTTOM_PANEL_MM = 32          # Tinggi area panel produk
WARNING_BANNER_H_MM = 6       # [UPDATE] Tinggi banner hitam sedikit diperbesar agar teks bold muat

# Line height rapat (3.6mm) & padding kanan
LINE_HEIGHT_MM, QTY_RIGHT_PAD_MM = 3.6, 3  

SCALE_LOGO_PATH, SCALE_LOGO_W_MM, SCALE_LOGO_H_MM = "scale_logo.png", 16, 8

QTY_COL_W_MM = 22
NAME_GAP_MM = 3

# Mode gambar resi: "contain" (aman) atau "cover" (full)
RESI_FIT_MODE = "contain"


# ================= UTILS =================
def safe_colname(c) -> str:
    c = str(c).strip()
    if c == "" or c.lower().startswith("unnamed"):
        return c
    return re.sub(r"\s+", " ", c)

def dedupe_columns(cols: List[str]) -> List[str]:
    seen, out = {}, []
    for c in cols:
        c0 = str(c).strip() or "Unnamed"
        if c0 not in seen:
            seen[c0] = 1
            out.append(c0)
        else:
            seen[c0] += 1
            out.append(f"{c0}__{seen[c0]}")
    return out

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [safe_colname(c) for c in df.columns]
    df.columns = dedupe_columns(list(df.columns))
    return df

def _fix_numeric_string(s: str) -> str:
    if not s:
        return s
    s0 = s.strip()
    if re.fullmatch(r"\d+\.0+", s0):
        return s0.split(".", 1)[0]
    if re.fullmatch(r"\d+(\.\d+)?[eE][\+\-]?\d+", s0):
        try:
            d = Decimal(s0)
            s_int = format(d, "f")
            if "." in s_int:
                s_int = s_int.split(".", 1)[0]
            return s_int
        except (InvalidOperation, ValueError):
            return s0
    return s0

def normalize_resi(s: str) -> str:
    if not s:
        return ""
    s = _fix_numeric_string(str(s).strip()).upper()
    return re.sub(r"[^A-Z0-9\-/]", "", s)

def canon_resi(s: str) -> str:
    s = normalize_resi(s)
    return re.sub(r"[-/]", "", s) if s else ""

def normalize_phone(p: str) -> str:
    if not p:
        return ""
    p = re.sub(r"[^\d\+]", "", str(p).strip()).replace("+", "")
    if not p:
        return ""
    if p.startswith("0"):
        p = "62" + p[1:]
    elif p.startswith("8"):
        p = "62" + p
    elif p.startswith("620"):
        p = "62" + p[3:]
    return p

def trunc(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"

def _find_col(df: pd.DataFrame, contains: List[str]) -> Optional[str]:
    for c in df.columns:
        cl = str(c).lower()
        if any(k in cl for k in contains):
            return c
    return None

def format_consumable_box(code_raw: str) -> str:
    code = (code_raw or "").strip()
    if not code:
        return "Consumable Box"
    u = code.upper()
    m_hb = re.match(r"^(\d{1,2})CBHB", u)
    if m_hb:
        n = m_hb.group(1)
        return f"Consumable Box HB {n} [{code}]"
    m_gm = re.match(r"^(\d{1,2})CBGM", u)
    if m_gm:
        n = m_gm.group(1)
        return f"Consumable Box GM {n} [{code}]"
    return f"Consumable Box [{code}]"


# ================= TEXT WRAP =================
def wrap_text_to_width(c: canvas.Canvas, text: str, max_w: float, font_name: str, font_size: float) -> List[str]:
    text = (text or "").strip()
    if not text:
        return [""]

    c.setFont(font_name, font_size)

    def fits(s: str) -> bool:
        return c.stringWidth(s, font_name, font_size) <= max_w

    words = text.split()
    lines: List[str] = []
    cur = ""

    for w in words:
        if not cur:
            if fits(w):
                cur = w
            else:
                chunk = ""
                for ch in w:
                    if fits(chunk + ch):
                        chunk += ch
                    else:
                        if chunk:
                            lines.append(chunk)
                        chunk = ch
                cur = chunk
        else:
            test = (cur + " " + w).strip()
            if fits(test):
                cur = test
            else:
                lines.append(cur)
                if fits(w):
                    cur = w
                else:
                    chunk = ""
                    for ch in w:
                        if fits(chunk + ch):
                            chunk += ch
                        else:
                            if chunk:
                                lines.append(chunk)
                            chunk = ch
                    cur = chunk

    if cur:
        lines.append(cur)
    return lines


# ================= GOOGLE SHEETS =================
def extract_gsheet_id_and_gid(url: str):
    if not url:
        return None, None
    url = url.strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    sheet_id = m.group(1) if m else None
    gid = None
    try:
        u = urlparse(url)
        q = parse_qs(u.query)
        if "gid" in q and q["gid"]:
            gid = q["gid"][0]
        if gid is None and u.fragment:
            frag = parse_qs(u.fragment)
            if "gid" in frag and frag["gid"]:
                gid = frag["gid"][0]
    except Exception:
        pass
    return sheet_id, gid

def _fetch_gsheet_csv_bytes(sheet_id: str, gid: str) -> bytes:
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv,application/octet-stream"}
    r = requests.get(export_url, params={"format": "csv", "gid": gid}, headers=headers, timeout=30)
    if r.status_code != 200 or "text/html" in (r.headers.get("content-type") or "").lower():
        raise PermissionError("Gagal ambil Google Sheets. Pastikan Anyone with the link -> Viewer.")
    return r.content

def fetch_gsheet_df(sheet_url: str) -> pd.DataFrame:
    sheet_id, gid = extract_gsheet_id_and_gid(sheet_url)
    if not sheet_id or not gid:
        raise ValueError("Gagal menemukan Sheet ID atau GID dari link.")
    df = pd.read_csv(io.BytesIO(_fetch_gsheet_csv_bytes(sheet_id, gid)), dtype=str, keep_default_na=False)
    return normalize_columns(df)


# ================= UPLOAD TABLE =================
def read_uploaded_table_bytes(filename: str, file_bytes: bytes, sheet_name=None) -> pd.DataFrame:
    name = filename.lower()
    bio = io.BytesIO(file_bytes)
    if name.endswith(".csv"):
        df = pd.read_csv(bio, dtype=str, keep_default_na=False)
    elif name.endswith(".tsv"):
        df = pd.read_csv(bio, sep="\t", dtype=str, keep_default_na=False)
    elif name.endswith(".xlsx") or name.endswith(".xls"):
        df = pd.read_excel(bio, sheet_name=sheet_name, dtype=str, keep_default_na=False)
    else:
        raise ValueError("Format tidak didukung. Pakai xlsx/xls/csv/tsv.")
    return normalize_columns(df)


# ================= PDF TEXT EXTRACT =================
def extract_page_text_strong(page: fitz.Page) -> str:
    parts = []
    try:
        t = (page.get_text("text") or "").strip()
        if t:
            parts.append(t)
    except Exception:
        pass
    try:
        blocks = page.get_text("blocks") or []
        bt = "\n".join([b[4] for b in blocks if len(b) > 4 and isinstance(b[4], str)]).strip()
        if bt:
            parts.append(bt)
    except Exception:
        pass
    try:
        words = page.get_text("words") or []
        wt = " ".join([w[4] for w in words if len(w) > 4 and isinstance(w[4], str)]).strip()
        if wt:
            parts.append(wt)
    except Exception:
        pass
    return re.sub(r"\n{3,}", "\n\n", "\n".join([p for p in parts if p])).strip()

def _page_content_rect(page: fitz.Page, pad: float = 6.0) -> fitz.Rect:
    """
    Cari bounding box konten (gabungan text blocks) supaya whitespace besar kepotong.
    pad dalam POINT (bukan mm). Default 6pt ~ 2.1mm.
    """
    rect = None
    
    try:
        # 1. Text & Image Blocks
        blocks = page.get_text("blocks") or []
        for b in blocks:
            if len(b) < 4:
                continue
            x0, y0, x1, y1 = b[0], b[1], b[2], b[3]
            if x1 > x0 and y1 > y0:
                r = fitz.Rect(x0, y0, x1, y1)
                rect = r if rect is None else (rect | r)
    except Exception:
        pass
        
    try:
        # 2. Vector Drawings (Barcode sering berupa garis/vector)
        drawings = page.get_drawings() or []
        for d in drawings:
            dr = d.get("rect")
            if dr and dr.x1 > dr.x0 and dr.y1 > dr.y0:
                rect = dr if rect is None else (rect | dr)
    except Exception:
        pass

    if rect is None:
        return page.rect

    rect = fitz.Rect(
        max(page.rect.x0, rect.x0 - pad),
        max(page.rect.y0, rect.y0 - pad),
        min(page.rect.x1, rect.x1 + pad),
        min(page.rect.y1, rect.y1 + pad),
    )
    return rect

def pdf_page_to_png_bytes(doc: fitz.Document, page0: int, zoom: float = 2.5) -> bytes:
    """
    Render PDF page -> PNG, tapi di-crop dulu ke area konten.
    Ini yang bikin label besar seperti gambar ke-2.
    """
    page = doc.load_page(page0)
    clip = _page_content_rect(page, pad=6.0)

    pix = page.get_pixmap(
        matrix=fitz.Matrix(zoom, zoom),
        alpha=False,
        clip=clip
    )
    return pix.tobytes("png")

def pdf_page_count(pdf_bytes: bytes) -> int:
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        return doc.page_count

def load_scale_logo_bytes() -> Optional[bytes]:
    try:
        with open(SCALE_LOGO_PATH, "rb") as f:
            return f.read()
    except Exception:
        return None


# ================= CANDIDATE EXTRACTORS =================
def extract_phone_candidates(text: str):
    if not text:
        return []
    raw = re.findall(r"(?:\+?62|0)?8\d{7,12}", re.sub(r"\s+", " ", re.sub(r"[^\d\+]", " ", text)))
    return list(dict.fromkeys(filter(None, map(normalize_phone, raw))))

def extract_name_candidates(text: str):
    if not text:
        return []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    candidates = []
    keys = ["nama", "penerima", "consignee", "recipient", "to:"]
    for i, ln in enumerate(lines):
        if any(k in ln.lower() for k in keys):
            m = re.split(r":", ln, maxsplit=1)
            if len(m) == 2 and m[1].strip() and len(m[1].strip()) >= 2:
                candidates.append(m[1].strip())
            if i + 1 < len(lines):
                nxt = lines[i + 1].strip()
                if nxt and not re.search(r"\d{5,}", nxt) and len(nxt) <= 80:
                    candidates.append(nxt)
    for ln in lines[:3]:
        if 2 <= len(ln) <= 80 and not re.search(r"\d{5,}", ln) and not any(
            x in ln.lower() for x in ["resi", "awb", "shipping", "ekspedisi"]
        ):
            candidates.append(ln)
    return list(dict.fromkeys(re.sub(r"\s+", " ", str(c).strip()) for c in candidates if str(c).strip()))

def extract_resi_candidates(text: str):
    if not text:
        return []
    t = text.upper()

    ez = re.findall(r"\b[A-Z0-9]{2,5}(?:-[A-Z0-9]{2,8}){1,3}\b", t)
    cands = re.findall(r"[A-Z0-9][A-Z0-9\-/]{7,28}", t)
    for s in re.findall(r"(?:[A-Z0-9]\s*){8,32}", t):
        s2 = re.sub(r"\s+", "", s)
        if 8 <= len(s2) <= 30:
            cands.append(s2)
    cands += re.findall(r"\b\d{10,20}\b", t)
    cands = ez + cands

    def looks_like_product_or_box(code: str) -> bool:
        u = code.replace("-", "").replace("/", "")
        if re.fullmatch(r"(GM|EC|GMP|PB|GN)\d{6,12}", u):
            return True
        if "CBHB" in u or "CBGM" in u:
            return True
        return False

    out, seen = [], set()
    for r0 in cands:
        r1 = normalize_resi(r0)
        if len(r1) < 6:
            continue
        if looks_like_product_or_box(r1):
            continue

        digit_cnt = sum(ch.isdigit() for ch in r1)
        alpha_cnt = sum(ch.isalpha() for ch in r1)
        has_dash = "-" in r1

        if has_dash:
            if len(r1) < 8:
                continue
            if (digit_cnt + alpha_cnt) < 8:
                continue
        else:
            if digit_cnt < 6:
                continue

        if r1 not in seen:
            seen.add(r1)
            out.append(r1)

        cr = canon_resi(r1)
        if cr and cr not in seen:
            seen.add(cr)
            out.append(cr)

    def rank(x: str):
        return (1 if "-" in x else 0, sum(c.isdigit() for c in x), len(x))

    out.sort(key=rank, reverse=True)
    return out


# ================= LOOKUP + MATCHING =================
def build_lookup_indexes(df: pd.DataFrame, col_resi: str, col_nama: str, col_telp: str):
    resi_index: Dict[str, List[int]] = {}
    phone_index: Dict[str, List[int]] = {}
    name_index: Dict[str, List[int]] = {}

    if col_resi in df.columns:
        for i, key in enumerate(df[col_resi].astype(str).fillna("").map(normalize_resi).tolist()):
            if not key:
                continue
            for k in {key, canon_resi(key)}:
                if not k:
                    continue
                resi_index.setdefault(k, []).append(i)

    if col_telp in df.columns:
        for i, p in enumerate(df[col_telp].astype(str).fillna("").map(normalize_phone).tolist()):
            if p:
                phone_index.setdefault(p, []).append(i)

    if col_nama in df.columns:
        for i, n in enumerate(
            df[col_nama].astype(str)
            .fillna("")
            .map(lambda x: re.sub(r"\s+", " ", str(x).strip().lower()))
            .tolist()
        ):
            if n:
                name_index.setdefault(n, []).append(i)

    return resi_index, phone_index, name_index

def match_pdf_candidates(
    cand_resi, cand_phone, cand_name, match_mode: str,
    resi_index: dict, phone_index: dict, name_index: dict,
    df: Optional[pd.DataFrame] = None, col_telp: str = "", col_nama: str = ""
):
    def score_row(i: int) -> int:
        s = 0
        if df is None:
            return s

        if col_telp and col_telp in df.columns and cand_phone:
            row_ph = normalize_phone(str(df.iloc[i].get(col_telp, "") or ""))
            if row_ph and row_ph in cand_phone:
                s += 100

        if col_nama and col_nama in df.columns and cand_name:
            row_nm = re.sub(r"\s+", " ", str(df.iloc[i].get(col_nama, "") or "").strip().lower())
            for nm in cand_name[:3]:
                nm2 = re.sub(r"\s+", " ", str(nm).strip().lower())
                if nm2 and row_nm and (nm2 in row_nm or row_nm in nm2):
                    s += 30
                    break
        return s

    def pick_best(idxs: List[int]) -> Optional[int]:
        if not idxs:
            return None
        if len(idxs) == 1:
            return idxs[0]
        scored = sorted(((score_row(i), i) for i in idxs), reverse=True)
        return scored[0][1]

    def by_resi():
        for r in cand_resi:
            if r in resi_index:
                idx = pick_best(resi_index[r])
                return idx, f"RESI:{r}"
        return None, None

    def by_phone():
        for ph in cand_phone:
            if ph in phone_index:
                idx = pick_best(phone_index[ph])
                return idx, f"TELP:{ph}"
        return None, None

    def by_name():
        for nm in cand_name:
            key = re.sub(r"\s+", " ", str(nm).strip().lower())
            if key in name_index and name_index[key]:
                return name_index[key][0], f"NAMA:{nm}"
        return None, None

    if match_mode == "Auto (Resi → Telp → Nama)" or match_mode == "Auto (Resi -> Telp -> Nama)":
        idx, by = by_resi()
        if idx is not None:
            return idx, by
        idx, by = by_phone()
        if idx is not None:
            return idx, by
        return by_name()

    if match_mode == "Resi saja":
        return by_resi()
    if match_mode == "Telp saja":
        return by_phone()
    return by_name()


# ================= PRODUCT PARSING =================
def build_product_lines_from_row(row: pd.Series, max_items: int = 10) -> Dict[str, List[Tuple[str, str]]]:
    merged = {"BOX": {}, "BARANG": {}, "HADIAH": {}}

    for i in range(1, 11):
        sku_val, qty_val = "", ""
        for c in row.index:
            c_clean = re.sub(r"\s+", "", str(c).lower().strip())
            if c_clean == f"produk{i}sku":
                sku_val = str(row[c]).strip()
            elif c_clean == f"produk{i}qty":
                qty_val = str(row[c]).strip()

        if not sku_val or sku_val.lower() == "nan":
            continue

        qty = int(qty_val) if qty_val and qty_val.isdigit() else 1
        sku_upper = sku_val.upper()

        cat, name = "BARANG", ""

        if ("CBHB" in sku_upper) or ("CBGM" in sku_upper) or ("BOX" in sku_upper) or ("KARDUS" in sku_upper):
            cat = "BOX"
            name = format_consumable_box(sku_val)

        elif any(k in sku_upper for k in ["TASBIH", "GLAS", "SPTL", "SHKR", "HADIAH", "BONUS", "TOPLES", "FLYER", "PANDUAN", "CSO", "CRM"]):
            cat = "HADIAH"
            if "TASBIH" in sku_upper:
                name = "Hadiah Tasbih Digital"
            elif "GLAS" in sku_upper:
                name = "Hadiah Gelas"
            elif "SPTL" in sku_upper:
                name = "Hadiah Spatula"
            elif "SHKR" in sku_upper:
                name = "Hadiah Shaker"
            elif "TOPLES" in sku_upper:
                name = "Hadiah Toples"
            elif "FLYER" in sku_upper:
                name = "Hadiah Flyer"
            elif "PANDUAN" in sku_upper:
                name = "Bonus Panduan"
            else:
                name = "Hadiah/Bonus"
            name = f"{name} [{sku_val}]"

        else:
            barang_keys = ["GMP", "GM", "EC", "PB", "GN"]
            for k in barang_keys:
                if k in sku_upper:
                    name = {
                        "GM": "GAMAMILK",
                        "EC": "ETACEFIT",
                        "GMP": "GAMAMILK PREMIUM",
                        "PB": "PHENOBODY",
                        "GN": "GNAIT",
                    }[k]
                    cat = "BARANG"
                    name = f"{name} [{sku_val}]"
                    break

        if not name:
            name = f"{sku_val}"

        merged[cat][name] = merged[cat].get(name, 0) + qty

    return {
        "BOX": [(trunc(k, 200), f"{v} pcs") for k, v in merged["BOX"].items()][:max_items],
        "BARANG": [(trunc(k, 200), f"{v} pcs") for k, v in merged["BARANG"].items()][:max_items],
        "HADIAH": [(trunc(k, 260), f"{v} pcs") for k, v in merged["HADIAH"].items()][:max_items],
    }


# ================= MASTER RULES =================
def _sig_from_row_products(row: pd.Series) -> str:
    items = []
    for i in range(1, 11):
        sku_val, qty_val = "", ""
        for c in row.index:
            c_clean = re.sub(r"\s+", "", str(c).lower().strip())
            if c_clean == f"produk{i}sku":
                sku_val = str(row[c]).strip()
            elif c_clean == f"produk{i}qty":
                qty_val = str(row[c]).strip()

        if not sku_val or sku_val.lower() == "nan":
            continue
        sku_upper = sku_val.upper()

        if ("CBHB" in sku_upper) or ("CBGM" in sku_upper) or ("BOX" in sku_upper) or ("KARDUS" in sku_upper):
            continue
        if any(k in sku_upper for k in ["TASBIH","GLAS","SPTL","SHKR","HADIAH","BONUS","TOPLES","FLYER","PANDUAN","CSO","CRM"]):
            continue

        qty = int(qty_val) if qty_val and qty_val.isdigit() else 1
        sku_canon = re.sub(r"[^A-Z0-9]", "", sku_upper)
        items.append((sku_canon, qty))

    items.sort(key=lambda x: (x[0], x[1]))
    return "|".join([f"{s}x{q}" for s, q in items])

def build_master_mapping(master_df: pd.DataFrame):
    df = normalize_columns(master_df.copy())

    col_sig = _find_col(df, ["sig", "signature", "rule", "key"])
    col_box = _find_col(df, ["box", "cbgm", "cbhb", "msku"])
    col_hadiah_sku = _find_col(df, ["hadiah sku", "bonus sku", "gift sku"])
    col_hadiah_nama = _find_col(df, ["hadiah", "bonus", "gift", "nama hadiah", "hadiah nama"])
    col_qty = _find_col(df, ["qty", "jumlah"])

    if not col_sig:
        raise ValueError("Master tidak punya kolom signature (sig/signature/rule/key).")

    master_map = {}
    stats = {"sig_lr": 0, "sig_rl": 0, "qty": 0, "box_msku": 0, "hadiah_nama": 0, "hadiah_sku": 0}

    for _, r in df.iterrows():
        sig = str(r.get(col_sig, "") or "").strip()
        if not sig:
            continue

        box_raw = str(r.get(col_box, "") or "").strip() if col_box else ""
        hadiah_nama = str(r.get(col_hadiah_nama, "") or "").strip() if col_hadiah_nama else ""
        hadiah_sku = str(r.get(col_hadiah_sku, "") or "").strip() if col_hadiah_sku else ""
        qty_raw = str(r.get(col_qty, "") or "").strip() if col_qty else ""

        qty = 1
        if qty_raw.isdigit():
            qty = int(qty_raw)
            stats["qty"] += 1

        master_map[sig] = {"box_code": box_raw, "hadiah_nama": hadiah_nama, "hadiah_sku": hadiah_sku, "qty": qty}

        if box_raw: stats["box_msku"] += 1
        if hadiah_nama: stats["hadiah_nama"] += 1
        if hadiah_sku: stats["hadiah_sku"] += 1

    stats["sig_lr"] = sum(1 for k in master_map.keys() if "|" in k)
    return master_map, stats

def build_product_lines_with_master(row: pd.Series, master_map: dict, max_items: int = 10) -> Dict[str, List[Tuple[str, str]]]:
    base = build_product_lines_from_row(row, max_items=max_items)
    sig = _sig_from_row_products(row)

    rule = master_map.get(sig)
    if not rule:
        return base

    qty_rule = int(rule.get("qty") or 1)

    box_code = (rule.get("box_code") or "").strip()
    if box_code:
        box_name = format_consumable_box(box_code)
        base["BOX"] = [(trunc(box_name, 200), f"{qty_rule} pcs")] + base["BOX"]
        base["BOX"] = base["BOX"][:max_items]

    hadiah_nama = (rule.get("hadiah_nama") or "").strip()
    hadiah_sku = (rule.get("hadiah_sku") or "").strip()
    if hadiah_nama or hadiah_sku:
        label = hadiah_nama if hadiah_nama else "Hadiah/Bonus"
        if hadiah_sku:
            label = f"{label} [{hadiah_sku}]"
        base["HADIAH"] = [(trunc(label, 260), f"{qty_rule} pcs")] + base["HADIAH"]
        base["HADIAH"] = base["HADIAH"][:max_items]

    return base


# ================= A6 EXPORT =================
def _fit_image_contain(img_w: float, img_h: float, box_w: float, box_h: float):
    """Contain (no crop)."""
    img_ar = img_w / (img_h or 1)
    box_ar = box_w / (box_h or 1)
    return (box_w, box_w / img_ar) if img_ar > box_ar else (box_h * img_ar, box_h)

def _fit_image_cover(img_w: float, img_h: float, box_w: float, box_h: float):
    """Cover (bisa crop tipis)."""
    img_ar = img_w / (img_h or 1)
    box_ar = box_w / (box_h or 1)
    return (box_h * img_ar, box_h) if img_ar > box_ar else (box_w, box_w / img_ar)

def export_pdf_a6_style_produk(pages: List[dict], scale_logo_bytes: Optional[bytes] = None) -> bytes:
    buf = io.BytesIO()
    page_w, page_h = A6_W_MM * mm, A6_H_MM * mm
    c = canvas.Canvas(buf, pagesize=(page_w, page_h))
    margin, bottom_panel_h = MARGIN_MM * mm, BOTTOM_PANEL_MM * mm

    for i, p in enumerate(pages):
        if i > 0:
            c.showPage()
            c.setPageSize((page_w, page_h))

        # --- BORDER KOTAK (Frame Garis Luar) ---
        border_gap = 1.5 * mm
        c.setStrokeColorRGB(0, 0, 0)
        c.setLineWidth(1.0)
        c.rect(border_gap, border_gap, page_w - 2 * border_gap, page_h - 2 * border_gap)

        resi_area_x, resi_area_y = margin, margin + bottom_panel_h
        resi_area_w, resi_area_h = page_w - 2 * margin, page_h - (2 * margin + bottom_panel_h)

        if scale_logo_bytes:
            try:
                logo_w, logo_h = SCALE_LOGO_W_MM * mm, SCALE_LOGO_H_MM * mm
                c.drawImage(
                    ImageReader(Image.open(io.BytesIO(scale_logo_bytes)).convert("RGBA")),
                    margin, page_h - margin - logo_h,
                    width=logo_w, height=logo_h,
                    mask="auto",
                )
            except Exception:
                pass

        # Binarize (threshold) gambar resi untuk printer thermal khusus
        # Supaya warna abu-abu (dari anti-aliasing) jadi hitam pekat,
        # mencegah hasil cetak buram/berbintik.
        raw_img = Image.open(io.BytesIO(p["png_bytes"])).convert("L")
        resi_img = raw_img.point(lambda x: 0 if x < 200 else 255).convert("RGB")
        img_w, img_h = resi_img.size

        if RESI_FIT_MODE.lower() == "cover":
            draw_w, draw_h = _fit_image_cover(img_w, img_h, resi_area_w, resi_area_h)
        else:
            draw_w, draw_h = _fit_image_contain(img_w, img_h, resi_area_w, resi_area_h)

        x = resi_area_x + (resi_area_w - draw_w) / 2
        y = resi_area_y + (resi_area_h - draw_h)
        c.drawImage(ImageReader(resi_img), x, y, width=draw_w, height=draw_h)

        panel_x = margin
        panel_w = page_w - 2 * margin
        panel_top_y = margin + bottom_panel_h - 2 * mm

        # --- [UPDATE] BANNER PERINGATAN (Footer Hitam) ---
        # Uppercase dan Bold sesuai request
        banner_text = "MOHON JANGAN DITERIMA JIKA PAKET RUSAK"
        banner_h = WARNING_BANNER_H_MM * mm
        banner_y = margin # Tepat di atas margin bawah

        # Gambar kotak hitam full panel
        c.setFillColorRGB(0, 0, 0)
        c.rect(panel_x, banner_y, panel_w, banner_h, fill=1, stroke=0)

        # Gambar teks putih bold di tengah
        c.setFillColorRGB(1, 1, 1) # Putih
        c.setFont("Helvetica-Bold", 9) # Font Bold & Ukuran pas
        
        # Center text positioning
        text_x = panel_x + (panel_w / 2)
        text_y = banner_y + 1.8 * mm # Adjust vertikal supaya pas tengah kotak 6mm
        c.drawCentredString(text_x, text_y, banner_text)

        # Set batas bawah list produk agar tidak menimpa banner (+ space dikit)
        panel_bottom_y = margin + banner_h + 4.0 * mm
        # -------------------------------------------------

        grouped = p.get("produk") or {"BOX": [], "BARANG": [], "HADIAH": []}
        rows = []
        for cat in ["BOX", "BARANG", "HADIAH"]:
            for nm, qt in grouped.get(cat, []):
                rows.append((nm, qt))

        c.setFillColorRGB(0, 0, 0)
        c.setFont("Helvetica-Bold", 8)
        c.drawString(panel_x, panel_top_y, "Produk")
        c.drawRightString(panel_x + panel_w, panel_top_y, "Jumlah")

        c.setStrokeColorRGB(0.75, 0.75, 0.75)
        c.setLineWidth(0.6)
        c.line(panel_x, panel_top_y - 2.2 * mm, panel_x + panel_w, panel_top_y - 2.2 * mm)

        y2 = panel_top_y - 6 * mm
        line_h = LINE_HEIGHT_MM * mm
        max_lines = max(1, int((y2 - panel_bottom_y) / line_h) + 1)

        font_name = "Helvetica"
        font_size = 7.0
        c.setFont(font_name, font_size)

        qty_right_x = panel_x + panel_w - (QTY_RIGHT_PAD_MM * mm)
        qty_col_w = QTY_COL_W_MM * mm
        gap_w = NAME_GAP_MM * mm
        name_max_w = panel_w - qty_col_w - gap_w

        used_lines = 0
        for (nm, qt) in rows:
            if used_lines >= max_lines:
                break

            wrapped = wrap_text_to_width(c, str(nm), name_max_w, font_name, font_size)

            for li, line in enumerate(wrapped):
                if used_lines >= max_lines:
                    break

                c.drawString(panel_x, y2, line)
                if li == 0:
                    c.drawRightString(qty_right_x, y2, str(qt))

                y2 -= line_h
                used_lines += 1

        import datetime
        jakarta_tz = datetime.timezone(datetime.timedelta(hours=7))
        ts_text = "Dicetak: " + datetime.datetime.now(jakarta_tz).strftime("%d-%m-%Y %H:%M:%S")
        resi_text = p.get("resi", "")
        if resi_text:
            resi_text = "No. Resi: " + resi_text
            
        c.setFillColorRGB(0.5, 0.5, 0.5)
        c.setFont("Helvetica", 5)
        c.drawString(panel_x, margin + banner_h + 1.2 * mm, ts_text)
        if resi_text:
            c.drawRightString(panel_x + panel_w, margin + banner_h + 1.2 * mm, resi_text)

    c.save()
    return buf.getvalue()


# ================= MATCH PDFs =================
def match_pdfs(
    df: pd.DataFrame,
    pdf_file_items: List[Tuple[str, bytes]],  # (filename, bytes)
    col_resi: str,
    col_nama: str,
    col_telp: str,
    match_mode: str,
    max_pages: int,
    resi_index: dict,
    phone_index: dict,
    name_index: dict,
    debug_collect: bool = False,
    debug_limit: int = 20
):
    docs, results, matched_items, debug_rows = {}, [], [], []
    page_total, found_order = 0, 0
    seen_idxs = set()

    for fname, b in pdf_file_items:
        try:
            docs[fname] = fitz.open(stream=b, filetype="pdf")
        except Exception as e:
            results.append({"pdf_file": fname, "page": "", "status": "ERROR_OPEN", "note": str(e)})

    for fname, doc in docs.items():
        if not doc:
            continue

        for p0 in range(min(len(doc), int(max_pages))):
            page_total += 1
            text = extract_page_text_strong(doc.load_page(p0))

            cand_resi = extract_resi_candidates(text)
            cand_phone = extract_phone_candidates(text)
            cand_name = extract_name_candidates(text)

            idx, by = match_pdf_candidates(
                cand_resi=cand_resi,
                cand_phone=cand_phone,
                cand_name=cand_name,
                match_mode=match_mode,
                resi_index=resi_index,
                phone_index=phone_index,
                name_index=name_index,
                df=df,
                col_telp=col_telp,
                col_nama=col_nama,
            )

            if debug_collect and len(debug_rows) < debug_limit:
                debug_rows.append({
                    "pdf_file": fname,
                    "page": p0 + 1,
                    "text_len": len(text),
                    "resi_candidates": ", ".join(cand_resi[:8]),
                    "telp_candidates": ", ".join(cand_phone[:6]),
                    "nama_candidates": ", ".join(cand_name[:4]),
                    "status": "MATCHED" if idx is not None else "NOT_MATCHED",
                    "by": by or ""
                })

            if idx is None:
                results.append({"pdf_file": fname, "page": p0 + 1, "status": "NOT_MATCHED", "note": ""})
                continue

            if idx in seen_idxs:
                results.append({"pdf_file": fname, "page": p0 + 1, "status": "NOT_MATCHED", "note": f"Duplicate. Already matched {by}"})
                continue

            seen_idxs.add(idx)

            found_order += 1
            m_resi = normalize_resi(df.iloc[int(idx)].get(col_resi, "") or "")
            matched_items.append({
                "pdf_file": fname,
                "page0": p0,
                "matched_idx": int(idx),
                "matched_by": by or "",
                "resi": m_resi,
                "found_order": found_order
            })
            results.append({"pdf_file": fname, "page": p0 + 1, "status": "MATCHED", "note": by or "", "resi": m_resi})

    for d in docs.values():
        try:
            d.close()
        except Exception:
            pass

    return (
        pd.DataFrame(results),
        pd.DataFrame(debug_rows) if debug_rows else None,
        matched_items,
        page_total
    )
