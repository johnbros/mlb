import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import time
import random

def get_start_times(urls):
    options = uc.ChromeOptions()
    options.headless = False
    driver = None

    try:
        driver = uc.Chrome(version_main=135, options=options)
        driver.set_page_load_timeout(200)
        for url in urls:
            try:
                driver.get(url)
                time.sleep(random.uniform(2.5, 5.0))
                soup = BeautifulSoup(driver.page_source, "html.parser")
                divs = soup.find_all("div")
                for div in divs:
                    if div.text.strip().startswith("Start Time:"):
                        yield url, div.text.strip()
                        break
                else:
                    yield url, None
            except Exception as e:
                yield url, f"[ERROR] {e}"
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                print(f"[WARN] Failed to quit driver cleanly: {e}")
