import os
import re
from typing import Dict, List, Optional, Tuple

import pymysql
from pymysql.cursors import DictCursor

# Koneksi MySQL XAMPP. Default = setelan bawaan XAMPP (root tanpa password).
DB_HOST = os.environ.get("PRODUK_DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("PRODUK_DB_PORT", "3306"))
DB_USER = os.environ.get("PRODUK_DB_USER", "root")
DB_PASSWORD = os.environ.get("PRODUK_DB_PASSWORD", "")
DB_NAME = os.environ.get("PRODUK_DB_NAME", "ptslu")

KATEGORI = ["BARANG", "HADIAH", "BOX"]
KODE_MAX_LEN = 64

# Data awal = aturan yang dulu tertulis di resi_engine.build_product_lines_from_row
SEED_RULES = [
    ("GMP", "GAMAMILK PREMIUM", "BARANG"),
    ("GMS", "GAMAMILK SEREAL", "BARANG"),
    ("GPE", "GAMAMILK PLUS ECO", "BARANG"),
    ("GP", "GAMAMILK PLUS", "BARANG"),
    ("GM", "GAMAMILK", "BARANG"),
    ("EC", "ETACEFIT", "BARANG"),
    ("PB", "PHENOBODY", "BARANG"),
    ("GN", "GNAIT", "BARANG"),
    ("TASBIH", "Hadiah Tasbih Digital", "HADIAH"),
    ("GLAS", "Hadiah Gelas", "HADIAH"),
    ("SPTL", "Hadiah Spatula", "HADIAH"),
    ("SHKR", "Hadiah Shaker", "HADIAH"),
    ("TOPLES", "Hadiah Toples", "HADIAH"),
    ("FLYER", "Hadiah Flyer", "HADIAH"),
    ("PANDUAN", "Bonus Panduan", "HADIAH"),
    ("TOTEBAG", "Hadiah Totebag", "HADIAH"),
    ("HADIAH", "Hadiah/Bonus", "HADIAH"),
    ("BONUS", "Hadiah/Bonus", "HADIAH"),
    ("CSO", "Hadiah/Bonus", "HADIAH"),
    ("CRM", "Hadiah/Bonus", "HADIAH"),
]

_initialized = False


class DBError(Exception):
    pass


def _connect(with_db: bool = True):
    try:
        return pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME if with_db else None,
            charset="utf8mb4",
            cursorclass=DictCursor,
            autocommit=True,
            connect_timeout=5,
        )
    except pymysql.err.OperationalError as exc:
        raise DBError(
            f"Tidak bisa konek ke database MySQL ({DB_HOST}:{DB_PORT}). "
            f"Pastikan MySQL sudah di-Start di XAMPP Control Panel. Detail: {exc.args[-1]}"
        )


def normalize_kode(kode: str) -> str:
    return re.sub(r"\s+", "", str(kode or "")).upper()


def init_db():
    global _initialized
    if _initialized:
        return

    conn = _connect(with_db=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            cur.execute(f"USE `{DB_NAME}`")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS produk_rules (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    kode VARCHAR(64) NOT NULL UNIQUE,
                    nama VARCHAR(255) NOT NULL,
                    kategori VARCHAR(16) NOT NULL DEFAULT 'BARANG',
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute("SELECT COUNT(*) AS n FROM produk_rules")
            if cur.fetchone()["n"] == 0:
                cur.executemany(
                    "INSERT INTO produk_rules (kode, nama, kategori) VALUES (%s, %s, %s)",
                    SEED_RULES,
                )
    finally:
        conn.close()
    _initialized = True


def _execute(sql: str, params=()) -> Tuple[List[Dict[str, object]], int]:
    init_db()
    conn = _connect()
    try:
        with conn.cursor() as cur:
            affected = cur.execute(sql, params)
            return list(cur.fetchall()), affected
    finally:
        conn.close()


def list_rules() -> List[Dict[str, object]]:
    rows, _ = _execute("SELECT * FROM produk_rules ORDER BY kategori, kode")
    return rows


def _validate(kode: str, nama: str, kategori: str) -> Tuple[str, str, str]:
    kode = normalize_kode(kode)
    nama = (nama or "").strip()
    kategori = (kategori or "BARANG").strip().upper()
    if len(kode) < 2:
        raise ValueError("Kode SKU minimal 2 karakter.")
    if len(kode) > KODE_MAX_LEN:
        raise ValueError(f"Kode SKU maksimal {KODE_MAX_LEN} karakter.")
    if not nama:
        raise ValueError("Nama produk wajib diisi.")
    if kategori not in KATEGORI:
        raise ValueError("Kategori tidak valid.")
    return kode, nama[:255], kategori


def add_rule(kode: str, nama: str, kategori: str):
    kode, nama, kategori = _validate(kode, nama, kategori)
    try:
        _execute("INSERT INTO produk_rules (kode, nama, kategori) VALUES (%s, %s, %s)", (kode, nama, kategori))
    except pymysql.err.IntegrityError:
        raise ValueError(f"Kode {kode} sudah ada. Edit data yang lama saja.")


def update_rule(rule_id: int, kode: str, nama: str, kategori: str):
    kode, nama, kategori = _validate(kode, nama, kategori)
    if not _execute("SELECT id FROM produk_rules WHERE id = %s", (int(rule_id),))[0]:
        raise ValueError("Data tidak ditemukan.")
    try:
        _execute(
            "UPDATE produk_rules SET kode = %s, nama = %s, kategori = %s WHERE id = %s",
            (kode, nama, kategori, int(rule_id)),
        )
    except pymysql.err.IntegrityError:
        raise ValueError(f"Kode {kode} sudah dipakai data lain.")


def delete_rule(rule_id: int):
    _execute("DELETE FROM produk_rules WHERE id = %s", (int(rule_id),))


def load_rules() -> List[Dict[str, object]]:
    """Aturan siap pakai untuk resolve_sku (sudah diurutkan)."""
    rules = list_rules()
    # Kode terpanjang (paling spesifik) dicek dulu; kalau sama panjang, HADIAH didahulukan seperti logika lama.
    rules.sort(key=lambda r: (-len(r["kode"]), 0 if r["kategori"] == "HADIAH" else 1, r["id"]))
    return rules


def resolve_sku(sku: str, rules: List[Dict[str, object]]) -> Optional[Tuple[str, str]]:
    """Return (kategori, nama tampil) untuk SKU, atau None kalau tidak ada aturan yang cocok."""
    sku = (sku or "").strip()
    sku_key = normalize_kode(sku)
    if not sku_key:
        return None

    match = next((r for r in rules if r["kode"] == sku_key), None)
    if match is None:
        match = next((r for r in rules if r["kode"] in sku_key), None)
    if match is None:
        return None

    nama = str(match["nama"])
    if nama.upper() != sku.upper():
        nama = f"{nama} [{sku}]"
    return str(match["kategori"]), nama
