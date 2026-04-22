import base64
import io
import re
from typing import Dict, List, Optional

import pandas as pd
import qrcode
from barcode import Code128
from barcode.writer import ImageWriter


def make_barcode_b64(data: str) -> str:
    rv = io.BytesIO()
    options = {
        "module_width": 0.2,
        "module_height": 7.0,
        "quiet_zone": 1.0,
        "font_size": 6,
        "text_distance": 1.0,
        "write_text": False,
    }
    Code128(str(data), writer=ImageWriter()).write(rv, options=options)
    return base64.b64encode(rv.getvalue()).decode("utf-8")


def make_qr_b64(data: str) -> str:
    qr = qrcode.QRCode(box_size=4, border=1)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    rv = io.BytesIO()
    img.save(rv, format="PNG")
    return base64.b64encode(rv.getvalue()).decode("utf-8")


def resolve_column_name(df_columns, candidate_list: List[str]) -> Optional[str]:
    cols = list(df_columns)

    for cand in candidate_list:
        if cand in cols:
            return cand

    lowered = {str(c).lower(): c for c in cols}
    for cand in candidate_list:
        if cand.lower() in lowered:
            return lowered[cand.lower()]

    for cand in candidate_list:
        key = cand.lower()
        for col in cols:
            if key in str(col).lower():
                return col
    return None


def get_google_sheet_data(url: str) -> Optional[pd.DataFrame]:
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    if not match:
        return None

    sheet_id = match.group(1)
    gid_match = re.search(r"[?&]gid=([0-9]+)", url)
    gid_param = f"&gid={gid_match.group(1)}" if gid_match else ""
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv{gid_param}"

    try:
        return pd.read_csv(csv_url)
    except Exception:
        return None


def _format_currency(raw_value) -> str:
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        return "Rp 0"
        
    if isinstance(raw_value, (int, float)):
        return f"Rp {int(raw_value):,}".replace(",", ".")
        
    s = str(raw_value).strip()
    clean_s = s.replace("Rp", "").strip()
    
    if "." in clean_s and "," in clean_s:
        if clean_s.rfind(".") > clean_s.rfind(","):
            clean_s = clean_s.replace(",", "")
        else:
            clean_s = clean_s.replace(".", "").replace(",", ".")
    else:
        if "," in clean_s:
            parts = clean_s.split(",")
            if len(parts[-1]) == 3 and len(parts) > 1:
                clean_s = clean_s.replace(",", "")
            else:
                clean_s = clean_s.replace(",", ".")
        elif "." in clean_s:
            parts = clean_s.split(".")
            if len(parts[-1]) == 3 and len(parts) > 1:
                clean_s = clean_s.replace(".", "")
            else:
                pass
                
    clean_s = re.sub(r'[^\d\.]', '', clean_s)
    
    try:
        val = float(clean_s)
        return f"Rp {int(val):,}".replace(",", ".")
    except ValueError:
        return str(raw_value)


def _map_courier_logo(courier_name: str) -> Optional[str]:
    if not courier_name:
        return None

    name = courier_name.upper()
    if "POS" in name:
        return "posid.png"
    if "JNE" in name:
        return "jne.png"
    if "J&T" in name or "JNT" in name:
        return "jnt.png"
    if "WAHANA" in name:
        return "wahana.png"
    if "NINJA" in name:
        return "ninja.png"
    if "LION" in name or "LIAN" in name:
        return "lion.png"
    return None


def process_dataframe(df: pd.DataFrame) -> List[Dict[str, object]]:
    candidates = {
        "nvid": ["Unique Code", "NVID", "UniqueCode", "id_order", "idorder", "OrderID", "No Order", "No Resi"],
        "product_name_1st": ["product_name_1st", "product", "Product", "product_name", "Produk 1 sku"],
        "gudang": ["Gudang", "gudang", "Warehouse"],
        "first_name": ["FIRST NAME", "FirstName", "NAME", "Recipient", "To", "Nama Penerima"],
        "contact": ["CONTACT*", "CONTACT", "Phone", "telp", "PhoneNumber", "No HP"],
        "address1": ["ADDRESS 1*", "Address", "Address1", "ReceiverAddress", "Alamat"],
        "kecamatan": ["KECAMATAN", "kecamatan", "Subdistrict", "Kec"],
        "city": ["kota/kabupaten", "CITY", "KOTA", "Kota", "Kabupaten", "kabupaten"],
        "province": ["Provinsi", "PROVINCE", "PROVINSI", "Province"],
        "ongkir": ["COD VALUE", "Ongkir", "ONGKIR", "ongkir", "Ongkir (ID)", "OngkirValue"],
        "harga_barang": ["Harga Barang", "Total Harga", "Total"],
        "comments": ["ISI PAKET", "Comments", "Notes", "Comment", "Isi Paket"],
        "courier": ["Ekspedisi", "ekspedisi", "Kurir", "kurir", "Courier", "Shipping", "Jasa Kirim"],
        "qty": ["JUMLAH BARANG", "JUMLAH", "Jumlah", "QTY", "Qty", "Quantity", "PCS", "Jml Barang", "Jml"],
        "product_qty_1st": ["product_qty_1st", "qty_1", "qty1", "QTY 1", "Produk 1 qty"],
        "product_name_2nd": ["product_name_2nd", "product_2", "product2", "Produk 2 sku"],
        "product_qty_2nd": ["product_qty_2nd", "qty_2", "qty2", "QTY 2", "Produk 2 qty"],
        "product_name_3rd": ["product_name_3rd", "product_3", "product3", "Produk 3 sku"],
        "product_qty_3rd": ["product_qty_3rd", "qty_3", "qty3", "QTY 3", "Produk 3 qty"],
        "product_name_4th": ["product_name_4th", "product_name_4rd", "product_4", "product4", "Produk 4 sku"],
        "product_qty_4th": ["product_qty_4th", "product_qty_4rd", "qty_4", "qty4", "QTY 4", "Produk 4 qty"],
        "product_name_5th": ["product_name_5th", "product_name_5rd", "product_5", "product5", "Produk 5 sku"],
        "product_qty_5th": ["product_qty_5th", "product_qty_5rd", "qty_5", "qty5", "QTY 5", "Produk 5 qty"],
    }

    resolved = {}
    for key, cand in candidates.items():
        resolved[key] = resolve_column_name(df.columns, cand)

    labels_data = []

    for idx, row in df.iterrows():
        def g(key: str) -> str:
            col = resolved.get(key)
            if not col:
                return ""
            val = row.get(col, "")
            return "" if pd.isna(val) else str(val).strip()

        if not g("first_name") and not g("contact") and not g("address1") and not g("nvid"):
            continue

        raw_code = g("nvid") or f"ROW-{idx}"
        if raw_code.lower().startswith("nvid"):
            raw_code = raw_code.split(":")[-1].strip()

        product_name_1st = g("product_name_1st")
        gudang = g("gudang")
        sender_fullname = f"{product_name_1st} {gudang}".strip() or "Gudang Pengirim"

        addr_parts = [g("address1")]
        region_parts = [v for v in [g("kecamatan"), g("city"), g("province")] if v]
        if region_parts:
            addr_parts.append(", ".join(region_parts))

        courier_name = g("courier")
        courier_logo = _map_courier_logo(courier_name)

        products = []
        p1_name = product_name_1st
        p1_qty = g("product_qty_1st")
        if p1_name or p1_qty:
            products.append({"name": p1_name, "qty": p1_qty or ""})

        p2_name = g("product_name_2nd")
        p2_qty = g("product_qty_2nd")
        if p2_name or p2_qty:
            products.append({"name": p2_name, "qty": p2_qty or ""})

        p3_name = g("product_name_3rd")
        p3_qty = g("product_qty_3rd")
        if p3_name or p3_qty:
            products.append({"name": p3_name, "qty": p3_qty or ""})

        p4_name = g("product_name_4th")
        p4_qty = g("product_qty_4th")
        if p4_name or p4_qty:
            products.append({"name": p4_name, "qty": p4_qty or ""})

        p5_name = g("product_name_5th")
        p5_qty = g("product_qty_5th")
        if p5_name or p5_qty:
            products.append({"name": p5_name, "qty": p5_qty or ""})

        primary_product_name = products[0]["name"] if products else product_name_1st
        qty_total = g("qty")
        primary_qty = products[0]["qty"] if products else qty_total

        ongkir_val = g("ongkir")
        if not ongkir_val or str(ongkir_val).strip() in ["0", "0.0", "-", "Rp 0", "NaN", "nan"]:
            fallback = g("harga_barang")
            if fallback:
                ongkir_val = fallback

        item = {
            "code_full": raw_code,
            "barcode_b64": make_barcode_b64(raw_code),
            "qr_b64": make_qr_b64(raw_code),
            "sender_name": sender_fullname,
            "sender_phone": "6289652269010",
            "sender_address_lines": [],
            "receiver_name": g("first_name"),
            "receiver_phone": g("contact"),
            "receiver_address_lines": addr_parts,
            "cod": _format_currency(ongkir_val),
            "comments": g("comments"),
            "courier": courier_name,
            "courier_logo": courier_logo,
            "qty": primary_qty,
            "product_name": primary_product_name,
            "products": products,
        }
        labels_data.append(item)

    return labels_data
