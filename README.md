# Dashboard Sidebar Python

Proyek ini adalah aplikasi Flask baru yang menggabungkan dua fungsi:

1. Label Generator (dari `dummmypos.ptslu.cloud`)
2. Resi Matcher A6 (dari `resi.ptslu.cloud`)

Semua modul dibungkus dalam satu dashboard dengan menu sidebar di kiri.

## Fitur

- Dashboard ringkasan
- Menu kiri: `Dashboard`, `Label Generator`, `Resi Matcher A6`, `Pengaturan API Key`
- Tema UI modern & responsive berbasis Tailwind CSS
- Input Google Sheet / Excel untuk label
- Matching PDF resi terhadap data order
- Output PDF A6 + report matching
- API key management untuk akses dari web lain (CORS + proteksi API key)
- Endpoint API:
  - `GET /api/ping`
  - `POST /api/label/generate`
  - `POST /api/resi/process`
  - `GET /api/resi/download/<job_id>`
  - `GET /api/resi/report/<job_id>`

## Jalankan Lokal

```bash
cd g
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Buka:

```text
http://127.0.0.1:5000
```

## Struktur Folder

```text
dashboard_sidebar_python/
  app.py
  requirements.txt
  modules/
    label_service.py
    resi_engine.py
  templates/
    base.html
    dashboard.html
    api_settings.html
    label.html
    label_preview.html
    resi.html
  static/
    img/*.png
```
