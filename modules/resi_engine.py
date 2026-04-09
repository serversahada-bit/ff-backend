import io
import re
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import fitz
import pandas as pd
import requests
from PIL import Image
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


A6_W_MM = 105
A6_H_MM = 148
MARGIN_MM = 3
BOTTOM_PANEL_MM = 32
WARNING_BANNER_H_MM = 6
LINE_HEIGHT_MM = 3.6
QTY_RIGHT_PAD_MM = 3
QTY_COL_W_MM = 22
NAME_GAP_MM = 3


def safe_colname(col_name) -> str:
    col_name = str(col_name).strip()
    if col_name == "" or col_name.lower().startswith("unnamed"):
        return col_name
    return re.sub(r"\s+", " ", col_name)


def dedupe_columns(columns: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for col in columns:
        col0 = str(col).strip() or "Unnamed"
        if col0 not in seen:
            seen[col0] = 1
            out.append(col0)
        else:
            seen[col0] += 1
            out.append(f"{col0}__{seen[col0]}")
    return out


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [safe_colname(c) for c in out.columns]
    out.columns = dedupe_columns(list(out.columns))
    return out


def _fix_numeric_string(value: str) -> str:
    if not value:
        return value
    value = value.strip()
    if re.fullmatch(r"\d+\.0+", value):
        return value.split(".", 1)[0]
    if re.fullmatch(r"\d+(\.\d+)?[eE][+\-]?\d+", value):
        try:
            dec = Decimal(value)
            text = format(dec, "f")
            if "." in text:
                text = text.split(".", 1)[0]
            return text
        except (InvalidOperation, ValueError):
            return value
    return value


def normalize_resi(value: str) -> str:
    if not value:
        return ""
    value = _fix_numeric_string(str(value).strip()).upper()
    return re.sub(r"[^A-Z0-9\-/]", "", value)


def canon_resi(value: str) -> str:
    resi = normalize_resi(value)
    return re.sub(r"[-/]", "", resi) if resi else ""


def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    phone = re.sub(r"[^\d\+]", "", str(phone).strip()).replace("+", "")
    if not phone:
        return ""
    if phone.startswith("0"):
        phone = "62" + phone[1:]
    elif phone.startswith("8"):
        phone = "62" + phone
    elif phone.startswith("620"):
        phone = "62" + phone[3:]
    return phone


def trunc(text: str, limit: int) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else text[: max(0, limit - 3)] + "..."


def format_consumable_box(code_raw: str) -> str:
    code = (code_raw or "").strip()
    if not code:
        return "Consumable Box"
    code_upper = code.upper()
    match_hb = re.match(r"^(\d{1,2})CBHB", code_upper)
    if match_hb:
        return f"Consumable Box HB {match_hb.group(1)} [{code}]"
    match_gm = re.match(r"^(\d{1,2})CBGM", code_upper)
    if match_gm:
        return f"Consumable Box GM {match_gm.group(1)} [{code}]"
    return f"Consumable Box [{code}]"


def wrap_text_to_width(
    pdf_canvas: canvas.Canvas,
    text: str,
    max_width: float,
    font_name: str,
    font_size: float,
) -> List[str]:
    text = (text or "").strip()
    if not text:
        return [""]

    pdf_canvas.setFont(font_name, font_size)

    def fits(value: str) -> bool:
        return pdf_canvas.stringWidth(value, font_name, font_size) <= max_width

    words = text.split()
    lines: List[str] = []
    current = ""

    for word in words:
        if not current:
            if fits(word):
                current = word
            else:
                chunk = ""
                for ch in word:
                    if fits(chunk + ch):
                        chunk += ch
                    else:
                        if chunk:
                            lines.append(chunk)
                        chunk = ch
                current = chunk
        else:
            test = f"{current} {word}".strip()
            if fits(test):
                current = test
            else:
                lines.append(current)
                if fits(word):
                    current = word
                else:
                    chunk = ""
                    for ch in word:
                        if fits(chunk + ch):
                            chunk += ch
                        else:
                            if chunk:
                                lines.append(chunk)
                            chunk = ch
                    current = chunk

    if current:
        lines.append(current)
    return lines


def extract_gsheet_id_and_gid(url: str):
    if not url:
        return None, None
    url = url.strip()
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    sheet_id = match.group(1) if match else None
    gid = None
    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        if "gid" in query and query["gid"]:
            gid = query["gid"][0]
        if gid is None and parsed.fragment:
            fragment = parse_qs(parsed.fragment)
            if "gid" in fragment and fragment["gid"]:
                gid = fragment["gid"][0]
    except Exception:
        pass
    return sheet_id, gid


def _fetch_gsheet_csv_bytes(sheet_id: str, gid: str) -> bytes:
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv,application/octet-stream"}
    response = requests.get(export_url, params={"format": "csv", "gid": gid}, headers=headers, timeout=30)
    content_type = (response.headers.get("content-type") or "").lower()
    if response.status_code != 200 or "text/html" in content_type:
        raise PermissionError("Gagal ambil Google Sheets. Pastikan akses link: Anyone with the link.")
    return response.content


def fetch_gsheet_df(sheet_url: str) -> pd.DataFrame:
    sheet_id, gid = extract_gsheet_id_and_gid(sheet_url)
    if not sheet_id or not gid:
        raise ValueError("Gagal menemukan Sheet ID atau GID dari link.")
    raw = _fetch_gsheet_csv_bytes(sheet_id, gid)
    df = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False)
    return normalize_columns(df)


def read_uploaded_table_bytes(filename: str, file_bytes: bytes, sheet_name=None) -> pd.DataFrame:
    lower = filename.lower()
    bio = io.BytesIO(file_bytes)
    if lower.endswith(".csv"):
        df = pd.read_csv(bio, dtype=str, keep_default_na=False)
    elif lower.endswith(".tsv"):
        df = pd.read_csv(bio, sep="\t", dtype=str, keep_default_na=False)
    elif lower.endswith(".xlsx") or lower.endswith(".xls"):
        df = pd.read_excel(bio, sheet_name=sheet_name, dtype=str, keep_default_na=False)
    else:
        raise ValueError("Format tidak didukung. Gunakan xlsx/xls/csv/tsv.")
    return normalize_columns(df)


def extract_page_text_strong(page: fitz.Page) -> str:
    parts = []
    try:
        text = (page.get_text("text") or "").strip()
        if text:
            parts.append(text)
    except Exception:
        pass
    try:
        blocks = page.get_text("blocks") or []
        block_text = "\n".join([b[4] for b in blocks if len(b) > 4 and isinstance(b[4], str)]).strip()
        if block_text:
            parts.append(block_text)
    except Exception:
        pass
    try:
        words = page.get_text("words") or []
        word_text = " ".join([w[4] for w in words if len(w) > 4 and isinstance(w[4], str)]).strip()
        if word_text:
            parts.append(word_text)
    except Exception:
        pass
    return re.sub(r"\n{3,}", "\n\n", "\n".join([p for p in parts if p])).strip()


def _page_content_rect(page: fitz.Page, pad: float = 6.0) -> fitz.Rect:
    rect = None
    try:
        blocks = page.get_text("blocks") or []
        for block in blocks:
            if len(block) < 4:
                continue
            x0, y0, x1, y1 = block[0], block[1], block[2], block[3]
            if x1 > x0 and y1 > y0:
                r = fitz.Rect(x0, y0, x1, y1)
                rect = r if rect is None else (rect | r)
    except Exception:
        pass

    try:
        drawings = page.get_drawings() or []
        for drawing in drawings:
            drect = drawing.get("rect")
            if drect and drect.x1 > drect.x0 and drect.y1 > drect.y0:
                rect = drect if rect is None else (rect | drect)
    except Exception:
        pass

    if rect is None:
        return page.rect

    return fitz.Rect(
        max(page.rect.x0, rect.x0 - pad),
        max(page.rect.y0, rect.y0 - pad),
        min(page.rect.x1, rect.x1 + pad),
        min(page.rect.y1, rect.y1 + pad),
    )


def pdf_page_to_png_bytes(doc: fitz.Document, page0: int, zoom: float = 2.5) -> bytes:
    page = doc.load_page(page0)
    clip = _page_content_rect(page, pad=6.0)
    pixmap = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False, clip=clip)
    return pixmap.tobytes("png")


def extract_phone_candidates(text: str) -> List[str]:
    if not text:
        return []
    compact = re.sub(r"\s+", " ", re.sub(r"[^\d\+]", " ", text))
    raw = re.findall(r"(?:\+?62|0)?8\d{7,12}", compact)
    unique = []
    seen = set()
    for item in raw:
        normalized = normalize_phone(item)
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(normalized)
    return unique


def extract_name_candidates(text: str) -> List[str]:
    if not text:
        return []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    candidates: List[str] = []
    keys = ["nama", "penerima", "consignee", "recipient", "to:"]

    for index, line in enumerate(lines):
        if any(key in line.lower() for key in keys):
            split_line = re.split(r":", line, maxsplit=1)
            if len(split_line) == 2 and split_line[1].strip() and len(split_line[1].strip()) >= 2:
                candidates.append(split_line[1].strip())
            if index + 1 < len(lines):
                nxt = lines[index + 1].strip()
                if nxt and not re.search(r"\d{5,}", nxt) and len(nxt) <= 80:
                    candidates.append(nxt)

    for line in lines[:3]:
        if 2 <= len(line) <= 80 and not re.search(r"\d{5,}", line):
            lowered = line.lower()
            if not any(x in lowered for x in ["resi", "awb", "shipping", "ekspedisi"]):
                candidates.append(line)

    output = []
    seen = set()
    for item in candidates:
        clean = re.sub(r"\s+", " ", str(item).strip())
        if clean and clean not in seen:
            seen.add(clean)
            output.append(clean)
    return output


def extract_resi_candidates(text: str) -> List[str]:
    if not text:
        return []
    upper = text.upper()

    easy = re.findall(r"\b[A-Z0-9]{2,5}(?:-[A-Z0-9]{2,8}){1,3}\b", upper)
    cands = re.findall(r"[A-Z0-9][A-Z0-9\-/]{7,28}", upper)
    cands += re.findall(r"\b\d{10,20}\b", upper)
    cands = easy + cands

    def looks_like_product_or_box(code: str) -> bool:
        compact = code.replace("-", "").replace("/", "")
        if re.fullmatch(r"(GM|EC|GMP|PB|GN)\d{6,12}", compact):
            return True
        if "CBHB" in compact or "CBGM" in compact:
            return True
        return False

    out: List[str] = []
    seen = set()
    for raw in cands:
        normalized = normalize_resi(raw)
        if len(normalized) < 6:
            continue
        if looks_like_product_or_box(normalized):
            continue

        digit_count = sum(ch.isdigit() for ch in normalized)
        alpha_count = sum(ch.isalpha() for ch in normalized)
        has_dash = "-" in normalized
        if has_dash:
            if len(normalized) < 8:
                continue
            if (digit_count + alpha_count) < 8:
                continue
        else:
            if digit_count < 6:
                continue

        if normalized not in seen:
            seen.add(normalized)
            out.append(normalized)

        canonical = canon_resi(normalized)
        if canonical and canonical not in seen:
            seen.add(canonical)
            out.append(canonical)

    out.sort(key=lambda item: (1 if "-" in item else 0, sum(c.isdigit() for c in item), len(item)), reverse=True)
    return out


def build_lookup_indexes(df: pd.DataFrame, col_resi: str, col_nama: str, col_telp: str):
    resi_index: Dict[str, List[int]] = {}
    phone_index: Dict[str, List[int]] = {}
    name_index: Dict[str, List[int]] = {}

    if col_resi in df.columns:
        values = df[col_resi].astype(str).fillna("").map(normalize_resi).tolist()
        for idx, key in enumerate(values):
            if not key:
                continue
            for variant in {key, canon_resi(key)}:
                if variant:
                    resi_index.setdefault(variant, []).append(idx)

    if col_telp in df.columns:
        values = df[col_telp].astype(str).fillna("").map(normalize_phone).tolist()
        for idx, key in enumerate(values):
            if key:
                phone_index.setdefault(key, []).append(idx)

    if col_nama in df.columns:
        values = (
            df[col_nama]
            .astype(str)
            .fillna("")
            .map(lambda x: re.sub(r"\s+", " ", str(x).strip().lower()))
            .tolist()
        )
        for idx, key in enumerate(values):
            if key:
                name_index.setdefault(key, []).append(idx)

    return resi_index, phone_index, name_index


def _normalize_match_mode(match_mode: str) -> str:
    mode = (match_mode or "").strip().lower()
    if "auto" in mode:
        return "auto"
    if "resi" in mode:
        return "resi"
    if "telp" in mode or "phone" in mode:
        return "telp"
    return "nama"


def match_pdf_candidates(
    cand_resi: List[str],
    cand_phone: List[str],
    cand_name: List[str],
    match_mode: str,
    resi_index: Dict[str, List[int]],
    phone_index: Dict[str, List[int]],
    name_index: Dict[str, List[int]],
    df: Optional[pd.DataFrame] = None,
    col_telp: str = "",
    col_nama: str = "",
):
    def score_row(row_idx: int) -> int:
        score = 0
        if df is None:
            return score
        if col_telp and col_telp in df.columns and cand_phone:
            row_phone = normalize_phone(str(df.iloc[row_idx].get(col_telp, "") or ""))
            if row_phone and row_phone in cand_phone:
                score += 100

        if col_nama and col_nama in df.columns and cand_name:
            row_name = re.sub(r"\s+", " ", str(df.iloc[row_idx].get(col_nama, "") or "").strip().lower())
            for name in cand_name[:3]:
                c_name = re.sub(r"\s+", " ", str(name).strip().lower())
                if c_name and row_name and (c_name in row_name or row_name in c_name):
                    score += 30
                    break
        return score

    def pick_best(indexes: List[int]) -> Optional[int]:
        if not indexes:
            return None
        if len(indexes) == 1:
            return indexes[0]
        scored = sorted(((score_row(i), i) for i in indexes), reverse=True)
        return scored[0][1]

    def by_resi():
        for resi in cand_resi:
            if resi in resi_index:
                idx = pick_best(resi_index[resi])
                return idx, f"RESI:{resi}"
        return None, None

    def by_phone():
        for phone in cand_phone:
            if phone in phone_index:
                idx = pick_best(phone_index[phone])
                return idx, f"TELP:{phone}"
        return None, None

    def by_name():
        for name in cand_name:
            key = re.sub(r"\s+", " ", str(name).strip().lower())
            if key in name_index and name_index[key]:
                return name_index[key][0], f"NAMA:{name}"
        return None, None

    mode = _normalize_match_mode(match_mode)
    if mode == "auto":
        idx, by = by_resi()
        if idx is not None:
            return idx, by
        idx, by = by_phone()
        if idx is not None:
            return idx, by
        return by_name()
    if mode == "resi":
        return by_resi()
    if mode == "telp":
        return by_phone()
    return by_name()


def build_product_lines_from_row(row: pd.Series, max_items: int = 10) -> Dict[str, List[Tuple[str, str]]]:
    merged: Dict[str, Dict[str, int]] = {"BOX": {}, "BARANG": {}, "HADIAH": {}}

    for index in range(1, 11):
        sku_val = ""
        qty_val = ""
        for col in row.index:
            clean_col = re.sub(r"\s+", "", str(col).lower().strip())
            if clean_col == f"produk{index}sku":
                sku_val = str(row[col]).strip()
            elif clean_col == f"produk{index}qty":
                qty_val = str(row[col]).strip()

        if not sku_val or sku_val.lower() == "nan":
            continue

        qty = int(qty_val) if qty_val and qty_val.isdigit() else 1
        sku_upper = sku_val.upper()
        category = "BARANG"
        name = ""

        if ("CBHB" in sku_upper) or ("CBGM" in sku_upper) or ("BOX" in sku_upper) or ("KARDUS" in sku_upper):
            category = "BOX"
            name = format_consumable_box(sku_val)
        elif any(k in sku_upper for k in ["TASBIH", "GLAS", "SPTL", "SHKR", "HADIAH", "BONUS", "TOPLES", "FLYER", "PANDUAN", "CSO", "CRM"]):
            category = "HADIAH"
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
            for key, alias in {
                "GM": "GAMAMILK",
                "EC": "ETACEFIT",
                "GMP": "GAMAMILK PREMIUM",
                "PB": "PHENOBODY",
                "GN": "GNAIT",
            }.items():
                if key in sku_upper:
                    category = "BARANG"
                    name = f"{alias} [{sku_val}]"
                    break

        if not name:
            name = sku_val

        merged[category][name] = merged[category].get(name, 0) + qty

    return {
        "BOX": [(trunc(k, 200), f"{v} pcs") for k, v in merged["BOX"].items()][:max_items],
        "BARANG": [(trunc(k, 200), f"{v} pcs") for k, v in merged["BARANG"].items()][:max_items],
        "HADIAH": [(trunc(k, 260), f"{v} pcs") for k, v in merged["HADIAH"].items()][:max_items],
    }


def _fit_image_contain(img_w: float, img_h: float, box_w: float, box_h: float):
    img_ar = img_w / (img_h or 1)
    box_ar = box_w / (box_h or 1)
    return (box_w, box_w / img_ar) if img_ar > box_ar else (box_h * img_ar, box_h)


def export_pdf_a6_style_produk(pages: List[dict], scale_logo_bytes: Optional[bytes] = None) -> bytes:
    buffer = io.BytesIO()
    page_w = A6_W_MM * mm
    page_h = A6_H_MM * mm
    pdf_canvas = canvas.Canvas(buffer, pagesize=(page_w, page_h))
    margin = MARGIN_MM * mm
    bottom_panel_h = BOTTOM_PANEL_MM * mm

    for page_idx, page_data in enumerate(pages):
        if page_idx > 0:
            pdf_canvas.showPage()
            pdf_canvas.setPageSize((page_w, page_h))

        border_gap = 1.5 * mm
        pdf_canvas.setStrokeColorRGB(0, 0, 0)
        pdf_canvas.setLineWidth(1.0)
        pdf_canvas.rect(border_gap, border_gap, page_w - 2 * border_gap, page_h - 2 * border_gap)

        resi_area_x = margin
        resi_area_y = margin + bottom_panel_h
        resi_area_w = page_w - (2 * margin)
        resi_area_h = page_h - (2 * margin + bottom_panel_h)

        if scale_logo_bytes:
            try:
                logo = Image.open(io.BytesIO(scale_logo_bytes)).convert("RGBA")
                pdf_canvas.drawImage(ImageReader(logo), margin, page_h - margin - (8 * mm), width=(16 * mm), height=(8 * mm), mask="auto")
            except Exception:
                pass

        raw_img = Image.open(io.BytesIO(page_data["png_bytes"])).convert("L")
        resi_img = raw_img.point(lambda x: 0 if x < 200 else 255).convert("RGB")
        img_w, img_h = resi_img.size
        draw_w, draw_h = _fit_image_contain(img_w, img_h, resi_area_w, resi_area_h)
        draw_x = resi_area_x + (resi_area_w - draw_w) / 2
        draw_y = resi_area_y + (resi_area_h - draw_h)
        pdf_canvas.drawImage(ImageReader(resi_img), draw_x, draw_y, width=draw_w, height=draw_h)

        panel_x = margin
        panel_w = page_w - 2 * margin
        panel_top_y = margin + bottom_panel_h - 2 * mm

        banner_h = WARNING_BANNER_H_MM * mm
        banner_y = margin
        pdf_canvas.setFillColorRGB(0, 0, 0)
        pdf_canvas.rect(panel_x, banner_y, panel_w, banner_h, fill=1, stroke=0)
        pdf_canvas.setFillColorRGB(1, 1, 1)
        pdf_canvas.setFont("Helvetica-Bold", 9)
        pdf_canvas.drawCentredString(panel_x + (panel_w / 2), banner_y + 1.8 * mm, "MOHON JANGAN DITERIMA JIKA PAKET RUSAK")

        panel_bottom_y = margin + banner_h + 4.0 * mm
        grouped = page_data.get("produk") or {"BOX": [], "BARANG": [], "HADIAH": []}
        rows: List[Tuple[str, str]] = []
        for category in ["BOX", "BARANG", "HADIAH"]:
            for name, qty in grouped.get(category, []):
                rows.append((name, qty))

        pdf_canvas.setFillColorRGB(0, 0, 0)
        pdf_canvas.setFont("Helvetica-Bold", 8)
        pdf_canvas.drawString(panel_x, panel_top_y, "Produk")
        pdf_canvas.drawRightString(panel_x + panel_w, panel_top_y, "Jumlah")
        pdf_canvas.setStrokeColorRGB(0.75, 0.75, 0.75)
        pdf_canvas.setLineWidth(0.6)
        pdf_canvas.line(panel_x, panel_top_y - 2.2 * mm, panel_x + panel_w, panel_top_y - 2.2 * mm)

        y_cursor = panel_top_y - 6 * mm
        line_h = LINE_HEIGHT_MM * mm
        max_lines = max(1, int((y_cursor - panel_bottom_y) / line_h) + 1)

        font_name = "Helvetica"
        font_size = 7.0
        pdf_canvas.setFont(font_name, font_size)
        qty_right_x = panel_x + panel_w - (QTY_RIGHT_PAD_MM * mm)
        qty_col_w = QTY_COL_W_MM * mm
        gap_w = NAME_GAP_MM * mm
        name_max_w = panel_w - qty_col_w - gap_w

        used_lines = 0
        for name, qty in rows:
            if used_lines >= max_lines:
                break
            wrapped_lines = wrap_text_to_width(pdf_canvas, str(name), name_max_w, font_name, font_size)
            for idx, line in enumerate(wrapped_lines):
                if used_lines >= max_lines:
                    break
                pdf_canvas.drawString(panel_x, y_cursor, line)
                if idx == 0:
                    pdf_canvas.drawRightString(qty_right_x, y_cursor, str(qty))
                y_cursor -= line_h
                used_lines += 1

        import datetime

        jakarta_tz = datetime.timezone(datetime.timedelta(hours=7))
        timestamp_text = datetime.datetime.now(jakarta_tz).strftime("%d-%m-%Y %H:%M:%S")
        pdf_canvas.setFillColorRGB(0.5, 0.5, 0.5)
        pdf_canvas.setFont("Helvetica", 5)
        pdf_canvas.drawString(panel_x, margin + banner_h + 1.2 * mm, f"Dicetak: {timestamp_text}")

        resi_text = page_data.get("resi", "")
        if resi_text:
            pdf_canvas.drawRightString(panel_x + panel_w, margin + banner_h + 1.2 * mm, f"No. Resi: {resi_text}")

    pdf_canvas.save()
    return buffer.getvalue()


def match_pdfs(
    df: pd.DataFrame,
    pdf_file_items: List[Tuple[str, bytes]],
    col_resi: str,
    col_nama: str,
    col_telp: str,
    match_mode: str,
    max_pages: int,
    resi_index: Dict[str, List[int]],
    phone_index: Dict[str, List[int]],
    name_index: Dict[str, List[int]],
    debug_collect: bool = False,
    debug_limit: int = 20,
):
    docs: Dict[str, Optional[fitz.Document]] = {}
    results: List[Dict[str, object]] = []
    matched_items: List[Dict[str, object]] = []
    debug_rows: List[Dict[str, object]] = []
    page_total = 0
    found_order = 0

    for filename, pdf_bytes in pdf_file_items:
        try:
            docs[filename] = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as exc:
            docs[filename] = None
            results.append({"pdf_file": filename, "page": "", "status": "ERROR_OPEN", "note": str(exc)})

    for filename, doc in docs.items():
        if not doc:
            continue
        for page0 in range(min(len(doc), int(max_pages))):
            page_total += 1
            text = extract_page_text_strong(doc.load_page(page0))
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
                debug_rows.append(
                    {
                        "pdf_file": filename,
                        "page": page0 + 1,
                        "text_len": len(text),
                        "resi_candidates": ", ".join(cand_resi[:8]),
                        "telp_candidates": ", ".join(cand_phone[:6]),
                        "nama_candidates": ", ".join(cand_name[:4]),
                        "status": "MATCHED" if idx is not None else "NOT_MATCHED",
                        "by": by or "",
                    }
                )

            if idx is None:
                results.append({"pdf_file": filename, "page": page0 + 1, "status": "NOT_MATCHED", "note": ""})
                continue

            found_order += 1
            matched_resi = normalize_resi(df.iloc[int(idx)].get(col_resi, "") or "")
            matched_items.append(
                {
                    "pdf_file": filename,
                    "page0": page0,
                    "matched_idx": int(idx),
                    "matched_by": by or "",
                    "resi": matched_resi,
                    "found_order": found_order,
                }
            )
            results.append(
                {
                    "pdf_file": filename,
                    "page": page0 + 1,
                    "status": "MATCHED",
                    "note": by or "",
                    "resi": matched_resi,
                }
            )

    for doc in docs.values():
        try:
            if doc:
                doc.close()
        except Exception:
            pass

    debug_df = pd.DataFrame(debug_rows) if debug_rows else None
    return pd.DataFrame(results), debug_df, matched_items, page_total
