from playwright.sync_api import sync_playwright
import re
import os
import urllib.request
from urllib.error import URLError

def scrape_data(username, password, login_url, target_url):
    print(f"[*] Memulai Robot Playwright untuk: {username}")
    awb_data_list = []
    
    # Buat folder 'downloads' jika belum ada
    download_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "downloads")
    os.makedirs(download_dir, exist_ok=True)

    
    with sync_playwright() as p:
        # headless=True agar berjalan tersembunyi (seperti tidak buka Chrome)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        page = context.new_page()
        
        try:
            print(f"[*] Mengakses Portal Winning House: {login_url}")
            page.goto(login_url, wait_until="networkidle")
            
            print("[+] Mengisi email dan password otomatis...")
            # Form login Livewire WinningHouse
            page.wait_for_selector('#email', timeout=15000)
            page.fill('#email', username)
            page.fill('#password', password)
            
            print("[*] Mengklik tombol masuk...")
            page.click('button[type="submit"]')
            
            print("[*] Menembus Single Page Application (Livewire) selama 4 detik...")
            # Sengaja tunggu 4 detik karena login SPA merender ulang tanpa ganti url penuh
            page.wait_for_timeout(4000)
            
            if "client/awb-documents" not in page.url:
                print(f"[*] Berpindah ke menu AWB Documents secara paksa...")
                page.goto(target_url, wait_until="domcontentloaded")
                page.wait_for_timeout(3500)
            
            print("[+] Berhasil mengakses halaman Data. Mencari struktur AWB dan Tanggalnya...")
            
            # Kita menggunakan Javascript DOM Evaluation agar bisa mencari elemen container (Baris Tabel) dari AWB
            # Hal ini memungkinkan kita menarik TANGGAL yang bersesuaian pada baris yang sama.
            js_script = r'''() => {
                let results = [];
                let seen = new Set();
                const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
                let node;
                while (node = walker.nextNode()) {
                    const text = node.textContent;
                    const match = text.match(/AWB\d+/);
                    if (match) {
                        const awb_id = match[0];
                        if (seen.has(awb_id)) continue;
                        seen.add(awb_id);
                        
                        let container = node.parentElement.closest('tr');
                        if (!container) container = node.parentElement.closest('.card') || node.parentElement.closest('div[class*="border"]') || node.parentElement.parentElement;
                        
                        let downloadUrl = "";
                        if (container) {
                            let btn = container.querySelector('button[data-tip="Download PDF"]');
                            if (btn && btn.getAttribute('onclick')) {
                                let matchUrl = btn.getAttribute('onclick').match(/window\.open\('([^']+)'/);
                                if (matchUrl && matchUrl[1]) {
                                    downloadUrl = matchUrl[1];
                                }
                            }
                        }

                        let containerText = container ? container.innerText : "";
                        
                        let dateMatch = containerText.match(/\d{4}-\d{2}-\d{2}/) || 
                                        containerText.match(/\d{2}[\/\-]\d{2}[\/\-]\d{2,4}/) ||
                                        containerText.match(/\d{1,2}\s+(Jan|Feb|Mar|Apr|Mei|Jun|Jul|Agu|Sep|Okt|Nov|Des)[a-z]*\s+\d{2,4}/i);
                        
                        results.push({
                            id: awb_id,
                            date: dateMatch ? dateMatch[0] : "Otomatis",
                            download_url: downloadUrl
                        });
                    }
                }
                return results;
            }'''
            
            extracted_items = page.evaluate(js_script)
            
            if not extracted_items:
                print("[-] Data AWB di halaman ini tampak kosong.")
            else:
                print(f"[+] Hore! Ditemukan {len(extracted_items)} dokumen AWB sungguhan beserta tanggalnya!")
                for item in extracted_items:
                    download_url = item.get("download_url", "")
                    filename = f"Dokumen_{item['id']}_Export.pdf"
                    file_path = os.path.join(download_dir, filename)
                    
                    status_note = "Berhasil tersinkronisasi dari web WinningHouse"
                    
                    # Coba download PDF jika ada link URL nya
                    if download_url:
                        try:
                            print(f"  -> Mengunduh PDF untuk {item['id']} ...")
                            urllib.request.urlretrieve(download_url, file_path)
                            print(f"     [OK] Tersimpan di: {file_path}")
                            status_note += " & File PDF berhasil diunduh."
                        except Exception as e:
                            print(f"     [FAILED] Gagal mengunduh {item['id']}: {e}")
                            status_note += " (Namun gagal mengunduh PDF)."
                    else:
                        status_note += " (Tidak ditemukan link PDF)."

                    awb_data_list.append({
                        "id": item["id"],
                        "filename": filename,
                        "status_code": "LIVE DATA",
                        "date": item["date"],
                        "note": status_note,
                        "printed": 1,
                        "download_url": download_url
                    })
                    
        except Exception as e:
            print(f"[!] Terjadi masalah pada Robot Browser: {e}")
            
        finally:
            browser.close()
            
    return awb_data_list
