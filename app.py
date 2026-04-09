import os
import json
import re
import secrets
import uuid
from collections import defaultdict

import fitz
import pandas as pd
from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, session, url_for

from modules import label_service
from modules import resi_engine as eng


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
TMP_DIR = os.path.join(BASE_DIR, "tmp")
API_SETTINGS_PATH = os.path.join(BASE_DIR, "api_settings.json")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("DASHBOARD_SECRET_KEY", "change-this-secret")
app.config["UPLOAD_FOLDER"] = UPLOAD_DIR
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024

SIDEBAR_ITEMS = [
    {"key": "dashboard", "endpoint": "dashboard", "label": "Dashboard"},
    {"key": "label", "endpoint": "label_page", "label": "Label Generator"},
    {"key": "resi", "endpoint": "resi_page", "label": "Resi Matcher A6"},
    {"key": "api_settings", "endpoint": "api_settings_page", "label": "Pengaturan API Key"},
]

RESI_KEYS = {
    "job_id": "resi_job_id",
    "order_path": "resi_order_path",
    "report_path": "resi_report_path",
    "out_pdf_path": "resi_out_pdf_path",
    "preview_pdf_path": "resi_preview_pdf_path",
}

DEFAULT_API_SETTINGS = {
    "api_enabled": True,
    "api_key": "",
    "allowed_origins": ["*"],
}


@app.context_processor
def inject_sidebar_items():
    return {"sidebar_items": SIDEBAR_ITEMS}


def _normalize_origins(raw_text: str):
    if not raw_text:
        return ["*"]
    items = []
    for part in raw_text.replace(",", "\n").splitlines():
        origin = part.strip()
        if origin:
            items.append(origin)
    if not items:
        return ["*"]
    seen = set()
    out = []
    for origin in items:
        if origin not in seen:
            seen.add(origin)
            out.append(origin)
    return out


def _load_api_settings():
    data = dict(DEFAULT_API_SETTINGS)
    if os.path.isfile(API_SETTINGS_PATH):
        try:
            with open(API_SETTINGS_PATH, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
            if isinstance(loaded, dict):
                data.update(loaded)
        except Exception:
            pass

    if not data.get("api_key"):
        data["api_key"] = secrets.token_urlsafe(32)
        _save_api_settings(data)

    allowed = data.get("allowed_origins")
    if isinstance(allowed, str):
        allowed = _normalize_origins(allowed)
    elif isinstance(allowed, list):
        allowed = _normalize_origins("\n".join([str(v) for v in allowed]))
    else:
        allowed = ["*"]
    data["allowed_origins"] = allowed
    return data


def _save_api_settings(settings: dict):
    payload = {
        "api_enabled": bool(settings.get("api_enabled", True)),
        "api_key": str(settings.get("api_key") or "").strip(),
        "allowed_origins": settings.get("allowed_origins") or ["*"],
    }
    with open(API_SETTINGS_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True)


def _resolve_allowed_origin(origin: str, allowed_origins):
    if not origin:
        return "*"
    if not allowed_origins or "*" in allowed_origins:
        return "*"
    if origin in allowed_origins:
        return origin
    return None


def _api_error(message: str, status: int = 400):
    return jsonify({"ok": False, "message": message}), status


def _extract_api_key():
    api_key = (request.headers.get("X-API-Key") or "").strip()
    if not api_key:
        auth = (request.headers.get("Authorization") or "").strip()
        if auth.lower().startswith("bearer "):
            api_key = auth[7:].strip()
    if not api_key:
        api_key = (request.args.get("api_key") or "").strip()
    return api_key


def _require_api_access():
    settings = _load_api_settings()
    if not settings.get("api_enabled", True):
        return _api_error("API saat ini dinonaktifkan.", 403)

    allowed_origins = settings.get("allowed_origins") or ["*"]
    origin = request.headers.get("Origin")
    if origin and "*" not in allowed_origins and origin not in allowed_origins:
        return _api_error("Origin tidak diizinkan.", 403)

    incoming_key = _extract_api_key()
    if not incoming_key or incoming_key != settings.get("api_key"):
        return _api_error("API key tidak valid.", 401)
    return None


def _read_label_dataframe_from_request():
    df = None
    sheet_url = request.form.get("sheet_url", "").strip()
    if sheet_url:
        df = label_service.get_google_sheet_data(sheet_url)
        if df is None:
            raise ValueError("Gagal membaca Google Sheet. Pastikan file bisa diakses publik.")
        return df

    uploaded = request.files.get("file")
    if uploaded and uploaded.filename:
        try:
            return pd.read_excel(uploaded.stream, engine="openpyxl")
        except Exception:
            temp_path = os.path.join(app.config["UPLOAD_FOLDER"], uploaded.filename)
            uploaded.save(temp_path)
            try:
                return pd.read_excel(temp_path, engine="openpyxl")
            finally:
                if os.path.isfile(temp_path):
                    os.remove(temp_path)

    raise ValueError("Tidak ada input. Isi link Google Sheet atau upload file Excel.")


@app.get("/")
def dashboard():
    return render_template("dashboard.html", active_page="dashboard")


@app.get("/api-settings")
def api_settings_page():
    settings = _load_api_settings()
    api_key = settings.get("api_key", "")
    masked = f"{api_key[:8]}...{api_key[-6:]}" if len(api_key) > 16 else api_key

    return render_template(
        "api_settings.html",
        active_page="api_settings",
        api_enabled=settings.get("api_enabled", True),
        api_key=api_key,
        api_key_masked=masked,
        allowed_origins_text="\n".join(settings.get("allowed_origins", ["*"])),
        api_base_url=request.url_root.rstrip("/"),
    )


@app.post("/api-settings/save")
def api_settings_save():
    settings = _load_api_settings()

    action = (request.form.get("action") or "save").strip().lower()
    api_enabled = request.form.get("api_enabled") == "on"
    api_key = (request.form.get("api_key") or "").strip()

    if action == "regenerate":
        api_key = secrets.token_urlsafe(32)
        flash("API key baru berhasil dibuat.", "ok")
    elif not api_key:
        api_key = settings.get("api_key") or secrets.token_urlsafe(32)

    raw_origins = (request.form.get("allowed_origins") or "").strip()
    allowed_origins = _normalize_origins(raw_origins)

    payload = {
        "api_enabled": api_enabled,
        "api_key": api_key,
        "allowed_origins": allowed_origins,
    }
    _save_api_settings(payload)

    if action != "regenerate":
        flash("Pengaturan API berhasil disimpan.", "ok")
    return redirect(url_for("api_settings_page"))


@app.get("/label")
def label_page():
    return render_template("label.html", active_page="label")


@app.post("/label/preview")
def label_preview():
    try:
        df = _read_label_dataframe_from_request()
    except Exception as exc:
        return f"<h3>{exc}</h3>", 400

    labels = label_service.process_dataframe(df)
    from datetime import datetime

    return render_template(
        "label_preview.html",
        labels=labels,
        ts=datetime.now().strftime("%d/%m/%Y %H:%M"),
    )


def _get_resi_job_id() -> str:
    if RESI_KEYS["job_id"] not in session:
        session[RESI_KEYS["job_id"]] = uuid.uuid4().hex
    return session[RESI_KEYS["job_id"]]


def _resi_job_path(name: str) -> str:
    return os.path.join(TMP_DIR, f"{_get_resi_job_id()}_{name}")


def _save_df_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False, encoding="utf-8")


def _load_df_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _clear_resi_output():
    session.pop(RESI_KEYS["report_path"], None)
    session.pop(RESI_KEYS["out_pdf_path"], None)
    session.pop(RESI_KEYS["preview_pdf_path"], None)


def _build_resi_context():
    order_path = session.get(RESI_KEYS["order_path"])
    report_path = session.get(RESI_KEYS["report_path"])
    out_pdf_path = session.get(RESI_KEYS["out_pdf_path"])
    preview_pdf_path = session.get(RESI_KEYS["preview_pdf_path"])

    cols = []
    total_orders = 0
    if order_path and os.path.isfile(order_path):
        try:
            order_df = _load_df_csv(order_path)
            cols = list(order_df.columns)
            total_orders = len(order_df)
        except Exception:
            cols = []
            total_orders = 0

    rows = []
    matched = 0
    not_matched = 0
    total = 0
    if report_path and os.path.isfile(report_path):
        try:
            report_df = pd.read_csv(report_path, dtype=str, keep_default_na=False)
            total = len(report_df)
            if "status" in report_df.columns:
                matched = int((report_df["status"] == "MATCHED").sum())
                not_matched = int((report_df["status"] == "NOT_MATCHED").sum())
            rows = report_df.tail(200).to_dict(orient="records")
        except Exception:
            rows = []
            matched = 0
            not_matched = 0
            total = 0

    return {
        "cols": cols,
        "total_orders": total_orders,
        "preview_ready": bool(preview_pdf_path and os.path.isfile(preview_pdf_path)),
        "report_ready": bool(report_path and os.path.isfile(report_path)),
        "out_ready": bool(out_pdf_path and os.path.isfile(out_pdf_path)),
        "rows": rows,
        "total": total,
        "matched": matched,
        "not_matched": not_matched,
    }


def _read_resi_process_inputs(df: pd.DataFrame):
    col_resi = request.form.get("col_resi", "")
    col_nama = request.form.get("col_nama", "")
    col_telp = request.form.get("col_telp", "")
    match_mode = request.form.get("match_mode", "Auto (Resi -> Telp -> Nama)")

    try:
        max_pages = int(request.form.get("max_pages", "200") or 200)
    except Exception:
        max_pages = 200

    try:
        zoom = float(request.form.get("zoom", "3.5") or 3.5)
    except Exception:
        zoom = 3.5

    try:
        max_produk = int(request.form.get("max_produk", "6") or 6)
    except Exception:
        max_produk = 6

    if col_resi not in df.columns or col_nama not in df.columns or col_telp not in df.columns:
        raise ValueError("Mapping kolom tidak valid. Pastikan pilih kolom dari dropdown.")

    pdf_files = request.files.getlist("pdf_files")
    if not pdf_files or not any(item.filename for item in pdf_files):
        raise ValueError("Upload minimal 1 file PDF.")

    pdf_items = []
    for item in pdf_files:
        if item and item.filename:
            pdf_items.append((item.filename, item.read()))
    if not pdf_items:
        raise ValueError("Tidak ada file PDF valid yang diupload.")

    return col_resi, col_nama, col_telp, match_mode, max_pages, zoom, max_produk, pdf_items


def _read_order_df_from_request():
    source = request.form.get("order_source", "upload")
    if source == "gsheet":
        url = (request.form.get("order_gsheet_url") or "").strip()
        if not url:
            raise ValueError("Link Google Sheets order masih kosong.")
        df = eng.fetch_gsheet_df(url)
    else:
        uploaded = request.files.get("order_file")
        if not uploaded or not uploaded.filename:
            raise ValueError("Upload file order dulu.")
        df = eng.read_uploaded_table_bytes(uploaded.filename, uploaded.read())

    df = df.dropna(how="all").reset_index(drop=True)
    if len(df) == 0:
        raise ValueError("Data order kosong.")
    return df


def _build_resi_output(df, col_resi, col_nama, col_telp, match_mode, max_pages, zoom, max_produk, pdf_items):
    resi_index, phone_index, name_index = eng.build_lookup_indexes(df, col_resi, col_nama, col_telp)
    results_df, _debug_df, matched_items, page_total = eng.match_pdfs(
        df=df,
        pdf_file_items=pdf_items,
        col_resi=col_resi,
        col_nama=col_nama,
        col_telp=col_telp,
        match_mode=match_mode,
        max_pages=max_pages,
        resi_index=resi_index,
        phone_index=phone_index,
        name_index=name_index,
        debug_collect=False,
        debug_limit=0,
    )

    docs = {}
    for filename, data in pdf_items:
        try:
            docs[filename] = fitz.open(stream=data, filetype="pdf")
        except Exception:
            docs[filename] = None

    groups = defaultdict(list)
    for item in matched_items:
        groups[item["resi"]].append(item)

    export_pages = []
    for resi_key in sorted(groups.keys()):
        sorted_group = sorted(groups[resi_key], key=lambda x: (x["pdf_file"], x["page0"], x["found_order"]))
        for item in sorted_group:
            doc = docs.get(item["pdf_file"])
            if not doc:
                continue
            png = eng.pdf_page_to_png_bytes(doc, item["page0"], zoom=zoom)
            row = df.iloc[int(item["matched_idx"])]
            produk_lines = eng.build_product_lines_from_row(row, max_items=max_produk)
            export_pages.append({"png_bytes": png, "produk": produk_lines, "resi": item["resi"]})

    for doc in docs.values():
        try:
            if doc:
                doc.close()
        except Exception:
            pass

    if not export_pages:
        return {
            "ok": False,
            "message": "0 MATCHED. Kemungkinan PDF scan gambar atau data tidak cocok.",
            "results_df": results_df,
            "page_total": page_total,
            "out_pdf_bytes": None,
            "output_pages": 0,
        }

    out_pdf_bytes = eng.export_pdf_a6_style_produk(export_pages, None)
    return {
        "ok": True,
        "message": "Sukses.",
        "results_df": results_df,
        "page_total": page_total,
        "out_pdf_bytes": out_pdf_bytes,
        "output_pages": len(export_pages),
    }


@app.get("/resi")
def resi_page():
    context = _build_resi_context()
    return render_template("resi.html", active_page="resi", **context)


@app.post("/resi/load_order")
def resi_load_order():
    _clear_resi_output()

    try:
        df = _read_order_df_from_request()

        path = _resi_job_path("order.csv")
        _save_df_csv(df, path)
        session[RESI_KEYS["order_path"]] = path

        flash(f"Order loaded: {len(df)} baris.", "ok")
    except Exception as exc:
        flash(f"Gagal load order: {exc}", "error")

    return redirect(url_for("resi_page") + "#generate")


@app.post("/resi/process")
def resi_process():
    order_path = session.get(RESI_KEYS["order_path"])
    if not order_path or not os.path.isfile(order_path):
        flash("Load data order dulu.", "error")
        return redirect(url_for("resi_page") + "#generate")

    df = _load_df_csv(order_path)
    _clear_resi_output()

    try:
        col_resi, col_nama, col_telp, match_mode, max_pages, zoom, max_produk, pdf_items = _read_resi_process_inputs(df)
        build_result = _build_resi_output(
            df=df,
            col_resi=col_resi,
            col_nama=col_nama,
            col_telp=col_telp,
            match_mode=match_mode,
            max_pages=max_pages,
            zoom=zoom,
            max_produk=max_produk,
            pdf_items=pdf_items,
        )

        report_path = _resi_job_path("report.csv")
        build_result["results_df"].to_csv(report_path, index=False, encoding="utf-8")
        session[RESI_KEYS["report_path"]] = report_path

        if not build_result["ok"]:
            flash(build_result["message"], "error")
            return redirect(url_for("resi_page") + "#report")

        out_pdf_path = _resi_job_path("output.pdf")
        with open(out_pdf_path, "wb") as handle:
            handle.write(build_result["out_pdf_bytes"])

        session[RESI_KEYS["out_pdf_path"]] = out_pdf_path
        session[RESI_KEYS["preview_pdf_path"]] = out_pdf_path

        flash(
            f"Sukses: total scan {build_result['page_total']} halaman, output {build_result['output_pages']} halaman A6.",
            "ok",
        )
        return redirect(url_for("resi_page") + "#preview")
    except Exception as exc:
        flash(f"Gagal proses: {exc}", "error")
        return redirect(url_for("resi_page") + "#generate")


@app.get("/resi/preview_pdf")
def resi_preview_pdf():
    path = session.get(RESI_KEYS["preview_pdf_path"])
    if not path or not os.path.isfile(path):
        return ("", 404)
    return send_file(path, mimetype="application/pdf", as_attachment=False)


@app.get("/resi/download")
def resi_download():
    path = session.get(RESI_KEYS["out_pdf_path"])
    if not path or not os.path.isfile(path):
        flash("Output PDF belum ada.", "error")
        return redirect(url_for("resi_page") + "#generate")

    import datetime

    jakarta_tz = datetime.timezone(datetime.timedelta(hours=7))
    ts = datetime.datetime.now(jakarta_tz).strftime("%Y%m%d_%H%M%S")
    return send_file(path, as_attachment=True, download_name=f"A6_STAMP_{ts}.pdf")


@app.get("/resi/reset")
def resi_reset():
    for key in RESI_KEYS.values():
        session.pop(key, None)
    flash("Reset OK.", "ok")
    return redirect(url_for("resi_page") + "#generate")


@app.after_request
def add_api_cors_headers(response):
    if request.path.startswith("/api/"):
        settings = _load_api_settings()
        origin = request.headers.get("Origin")
        allow_origin = _resolve_allowed_origin(origin, settings.get("allowed_origins", ["*"]))
        if allow_origin:
            response.headers["Access-Control-Allow-Origin"] = allow_origin
            response.headers["Vary"] = "Origin"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-API-Key, Authorization"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


@app.route("/api/<path:subpath>", methods=["OPTIONS"])
def api_preflight(subpath):
    return ("", 204)


@app.get("/api/ping")
def api_ping():
    access_error = _require_api_access()
    if access_error:
        return access_error
    return jsonify({"ok": True, "message": "API aktif"})


@app.post("/api/label/generate")
def api_label_generate():
    access_error = _require_api_access()
    if access_error:
        return access_error

    try:
        df = _read_label_dataframe_from_request()
        labels = label_service.process_dataframe(df)
        return jsonify(
            {
                "ok": True,
                "count": len(labels),
                "labels": labels,
            }
        )
    except Exception as exc:
        return _api_error(str(exc), 400)


@app.post("/api/resi/process")
def api_resi_process():
    access_error = _require_api_access()
    if access_error:
        return access_error

    try:
        df = _read_order_df_from_request()
        col_resi, col_nama, col_telp, match_mode, max_pages, zoom, max_produk, pdf_items = _read_resi_process_inputs(df)

        build_result = _build_resi_output(
            df=df,
            col_resi=col_resi,
            col_nama=col_nama,
            col_telp=col_telp,
            match_mode=match_mode,
            max_pages=max_pages,
            zoom=zoom,
            max_produk=max_produk,
            pdf_items=pdf_items,
        )

        results_df = build_result["results_df"]
        total = len(results_df)
        matched = int((results_df["status"] == "MATCHED").sum()) if "status" in results_df.columns else 0
        not_matched = int((results_df["status"] == "NOT_MATCHED").sum()) if "status" in results_df.columns else 0

        job_id = uuid.uuid4().hex
        report_path = os.path.join(TMP_DIR, f"api_{job_id}_report.csv")
        results_df.to_csv(report_path, index=False, encoding="utf-8")

        output_url = None
        if build_result["ok"]:
            output_path = os.path.join(TMP_DIR, f"api_{job_id}_output.pdf")
            with open(output_path, "wb") as handle:
                handle.write(build_result["out_pdf_bytes"])
            output_url = url_for("api_resi_download", job_id=job_id, _external=True)

        return jsonify(
            {
                "ok": bool(build_result["ok"]),
                "message": build_result["message"],
                "job_id": job_id,
                "summary": {
                    "total": total,
                    "matched": matched,
                    "not_matched": not_matched,
                    "scan_pages": build_result["page_total"],
                    "output_pages": build_result["output_pages"],
                },
                "report_download_url": url_for("api_resi_report", job_id=job_id, _external=True),
                "output_download_url": output_url,
                "report_preview": results_df.tail(100).to_dict(orient="records"),
            }
        )
    except Exception as exc:
        return _api_error(str(exc), 400)


@app.get("/api/resi/download/<job_id>")
def api_resi_download(job_id):
    access_error = _require_api_access()
    if access_error:
        return access_error

    safe_id = re.sub(r"[^a-zA-Z0-9]", "", job_id or "")
    path = os.path.join(TMP_DIR, f"api_{safe_id}_output.pdf")
    if not safe_id or not os.path.isfile(path):
        return _api_error("File output tidak ditemukan.", 404)
    return send_file(path, mimetype="application/pdf", as_attachment=True, download_name=f"A6_OUTPUT_{safe_id}.pdf")


@app.get("/api/resi/report/<job_id>")
def api_resi_report(job_id):
    access_error = _require_api_access()
    if access_error:
        return access_error

    safe_id = re.sub(r"[^a-zA-Z0-9]", "", job_id or "")
    path = os.path.join(TMP_DIR, f"api_{safe_id}_report.csv")
    if not safe_id or not os.path.isfile(path):
        return _api_error("File report tidak ditemukan.", 404)
    return send_file(path, mimetype="text/csv", as_attachment=True, download_name=f"RESI_REPORT_{safe_id}.csv")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
