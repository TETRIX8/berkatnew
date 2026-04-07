import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import re
import os
import random

BASE_URL = "https://berkat.ru"

CATEGORIES = [
    {"name": "Транспорт", "path": "/avto"},
    {"name": "Недвижимость", "path": "/nedvizhimost"},
    {"name": "Работа", "path": "/rabota"},
    {"name": "Услуги", "path": "/uslugi"},
    {"name": "Электроника", "path": "/elektronika"},
    {"name": "Бытовая техника", "path": "/bytovaja-tehnika"},
    {"name": "Дом и сад", "path": "/dom-i-sad"},
    {"name": "Личные вещи", "path": "/lichnye-veshhi"},
    {"name": "Хобби и отдых", "path": "/hobbi-i-otdyh"},
    {"name": "Животные", "path": "/zhivotnye"},
    {"name": "Для бизнеса", "path": "/dlja-biznesa"},
    {"name": "Сельхозпродукция", "path": "/selhozprodukcija"},
    {"name": "Разное", "path": "/raznoe"}
]

class Parser:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(10)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        self.seen_urls = set()

    async def fetch(self, session, url, retries=5):
        for attempt in range(retries):
            async with self.semaphore:
                try:
                    async with session.get(url, timeout=30) as r:
                        if r.status == 200:
                            return await r.text()
                        elif r.status == 429:
                            await asyncio.sleep((attempt + 1) * 5)
                except:
                    await asyncio.sleep(1)
        return None

    async def get_ads(self, session, category_obj, limit=None):
        url = BASE_URL + category_obj["path"]
        ads = []
        page = 1
        current = url
        
        while current:
            if limit and len(ads) >= limit:
                break
                
            print(f"[{category_obj['name']}] Страница {page} -> {current}")
            html = await self.fetch(session, current)
            if not html:
                break
                
            soup = BeautifulSoup(html, "html.parser")
            links = soup.select("h3 a[href], .board_item_title a[href]")
            if not links:
                links = [a for a in soup.find_all("a", href=True) if "/view/" in a["href"]]

            found_on_page = 0
            for link in links:
                href = link["href"]
                if not href.startswith("http"):
                    href = BASE_URL + (href if href.startswith("/") else "/" + href)
                
                if href in self.seen_urls:
                    continue
                
                self.seen_urls.add(href)
                title = link.get_text(strip=True)
                if not title or len(title) < 3: continue

                ads.append({
                    "title": title,
                    "url": href,
                    "category": category_obj["name"],
                    "category_url": url
                })
                found_on_page += 1
                if limit and len(ads) >= limit:
                    break

            print(f"  Найдено объявлений: {found_on_page}")
            
            next_btn = soup.select_one("a.next_page, a[rel='next']")
            if not next_btn:
                next_btn = soup.find("a", string=re.compile(r"Следующая|Следующая →|Next|>", re.I))
            
            if next_btn and next_btn.get("href"):
                href = next_btn["href"]
                current = BASE_URL + (href if href.startswith("/") else "/" + href)
                page += 1
            else:
                next_page_link = soup.find("a", string=str(page + 1))
                if next_page_link and next_page_link.get("href"):
                    href = next_page_link["href"]
                    current = BASE_URL + (href if href.startswith("/") else "/" + href)
                    page += 1
                else:
                    break
        return ads

    async def parse_details(self, session, ad):
        html = await self.fetch(session, ad["url"])
        if not html:
            return ad
        soup = BeautifulSoup(html, "html.parser")
        city = soup.find("span", class_="board_item_city")
        ad["location"] = city.get_text(strip=True) if city else ""
        desc = soup.find("div", class_="board_item_description") or soup.find("div", class_="text")
        ad["description"] = desc.get_text(strip=True) if desc else ""
        price = soup.find("div", class_="board_item_price") or soup.find("span", class_="price")
        ad["price"] = price.get_text(strip=True) if price else ""
        photos = []
        for img in soup.select(".fotorama img, .board_item_photos img, img[data-full]"):
            src = img.get("data-full") or img.get("src")
            if src:
                if not src.startswith("http"):
                    src = BASE_URL + src
                if src not in photos:
                    photos.append(src)
        ad["photos"] = photos
        date_info = soup.find("span", class_="board_item_date")
        ad["published_at"] = date_info.get_text(strip=True).replace("Дата:", "").strip() if date_info else ""
        hits = soup.find("span", class_="board_item_hits")
        if hits:
            m = re.search(r"\d+", hits.get_text())
            ad["views"] = int(m.group()) if m else 0
        else:
            ad["views"] = 0
        return ad

async def main():
    os.makedirs("parsed_data", exist_ok=True)
    parser = Parser()
    async with aiohttp.ClientSession(headers=parser.headers) as session:
        all_ads_brief = []
        for cat in CATEGORIES:
            ads = await parser.get_ads(session, cat)
            all_ads_brief.extend(ads)
        results = []
        batch_size = 25
        for i in range(0, len(all_ads_brief), batch_size):
            batch = all_ads_brief[i:i + batch_size]
            tasks = [parser.parse_details(session, ad) for ad in batch]
            res = await asyncio.gather(*tasks)
            results.extend(res)
            print(f"Обработано: {len(results)} / {len(all_ads_brief)}")
            await asyncio.sleep(0.3)
        output_file = "parsed_data/berkat_full_data.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print(f"ГОТОВО. Сохранено {len(results)} объявлений.")

if __name__ == "__main__":
    asyncio.run(main())
