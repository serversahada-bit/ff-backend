from playwright.sync_api import sync_playwright

p = sync_playwright().start()
browser = p.chromium.launch(headless=True)
page = browser.new_page()
try:
    page.goto('https://winninghousefulfillment.com/login', wait_until='networkidle')
    page.fill('#email', 'gamamilk@winninghouse.com')
    page.fill('#password', 'winning123')
    page.click('button[type="submit"]')
    page.wait_for_timeout(4000)
    page.goto('https://winninghousefulfillment.com/client/awb-documents', wait_until='domcontentloaded')
    page.wait_for_timeout(3500)
    with open('tmp/awb_page.html', 'w', encoding='utf-8') as f:
        f.write(page.content())
    print('HTML saved to tmp/awb_page.html')
except Exception as e:
    print(str(e))
finally:
    browser.close()
    p.stop()
