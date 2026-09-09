import asyncio
import json
import os
import re
from pathlib import Path
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

BASE_URL = "https://berkat.ru"
OUTPUT_DIR = Path("parsed_data")

CATEGORIES = [
    {"name": "Транспорт", "slug": "transport", "path": "/avto"},
    {"name": "Недвижимость", "slug": "real-estate", "path": "/nedvizhimost"},
    {"name": "Работа", "slug": "jobs", "path": "/rabota"},
    {"name": "Услуги", "slug": "services", "path": "/uslugi"},
    {"name": "Электроника", "slug": "electronics", "path": "/elektronika"},
    {"name": "Бытовая техника", "slug": "appliances", "path": "/bytovaja-tehnika"},
    {"name": "Дом и сад", "slug": "home-and-garden", "path": "/dom-i-sad"},
    {"name": "Личные вещи", "slug": "personal-items", "path": "/lichnye-veshhi"},
    {"name": "Хобби и отдых", "slug": "hobbies-and-leisure", "path": "/hobbi-i-otdyh"},
    {"name": "Животные", "slug": "animals", "path": "/zhivotnye"},
    {"name": "Для бизнеса", "slug": "business", "path": "/dlja-biznesa"},
    {"name": "Сельхозпродукция", "slug": "agriculture", "path": "/selhozprodukcija"},
    {"name": "Разное", "slug": "other", "path": "/raznoe"},
]


class Parser:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(10)
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            )
        }

    async def fetch(self, session, url, retries=5):
        for attempt in range(retries):
            try:
                async with self.semaphore:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            return await response.text()
                        if response.status == 429 or response.status >= 500:
                            wait = min(30, 2 ** attempt)
                            await asyncio.sleep(wait)
                        else:
                            print(f"HTTP {response.status}: {url}")
                            return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                if attempt == retries - 1:
                    print(f"Не удалось загрузить {url}: {error}")
                else:
                    await asyncio.sleep(min(30, 2 ** attempt))
        return None

    @staticmethod
    def absolute_url(href):
        return urljoin(BASE_URL, href)

    async def get_ads(self, session, category):
        current = urljoin(BASE_URL, category["path"])
        page = 1
        ads = []
        seen_urls = set()

        while current:
            print(f"[{category['name']}] Страница {page} -> {current}")
            html = await self.fetch(session, current)
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")
            links = soup.select("h3 a[href], .board_item_title a[href]")
            if not links:
                links = [a for a in soup.find_all("a", href=True) if "/view/" in a["href"]]

            found = 0
            for link in links:
                href = self.absolute_url(link["href"])
                title = link.get_text(" ", strip=True)
                if href in seen_urls or not title or len(title) < 3:
                    continue
                seen_urls.add(href)
                ads.append(
                    {
                        "title": title,
                        "url": href,
                        "category": category["name"],
                        "category_slug": category["slug"],
                        "category_url": urljoin(BASE_URL, category["path"]),
                    }
                )
                found += 1

            print(f"  Найдено объявлений: {found}")
            next_btn = soup.select_one("a.next_page, a[rel='next']")
            if not next_btn:
                next_btn = soup.find("a", string=re.compile(r"Следующая|Next|>", re.I))
            if not next_btn or not next_btn.get("href"):
                break

            next_url = self.absolute_url(next_btn["href"])
            if next_url == current:
                break
            current = next_url
            page += 1

        return ads

    async def parse_details(self, session, ad):
        html = await self.fetch(session, ad["url"])
        if not html:
            return ad
        soup = BeautifulSoup(html, "html.parser")
        city = soup.find("span", class_="board_item_city")
        desc = soup.find("div", class_="board_item_desc") or soup.find("div", class_="board_item_description")
        price = soup.find("div", class_="board_item_price") or soup.find("span", class_="price")
        ad["location"] = city.get_text(" ", strip=True) if city else ""
        ad["description"] = desc.get_text(" ", strip=True) if desc else ""
        ad["price"] = price.get_text(" ", strip=True) if price else ""
        photo_sources = []
        for element in soup.select(
            ".fotorama a[href], .fotorama img, .board_item_photos a[href], "
            ".board_item_photos img, img[data-full]"
        ):
            source = element.get("data-full") or element.get("href") or element.get("src")
            if source:
                photo_sources.append(self.absolute_url(source))
        ad["photos"] = list(dict.fromkeys(photo_sources))
        date_info = soup.find("span", class_="board_item_date")
        ad["published_at"] = (
            date_info.get_text(" ", strip=True).replace("Дата:", "").strip() if date_info else ""
        )
        hits = soup.find("span", class_="board_item_hits")
        match = re.search(r"\d+", hits.get_text() if hits else "")
        ad["views"] = int(match.group()) if match else 0
        return ad


async def parse_category(parser, session, category):
    brief_ads = await parser.get_ads(session, category)
    results = []
    for start in range(0, len(brief_ads), 25):
        batch = brief_ads[start : start + 25]
        results.extend(await asyncio.gather(*(parser.parse_details(session, ad) for ad in batch)))
        print(f"[{category['name']}] Обработано: {len(results)} / {len(brief_ads)}")
        await asyncio.sleep(0.3)
    return category, results


async def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for old_file in OUTPUT_DIR.glob("*.json"):
        old_file.unlink()

    parser = Parser()
    async with aiohttp.ClientSession(headers=parser.headers) as session:
        parsed = await asyncio.gather(
            *(parse_category(parser, session, category) for category in CATEGORIES)
        )

    index = {
        "name": "Berkat.ru parsed data",
        "updated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "source": BASE_URL,
        "categories": [],
    }
    for category, records in parsed:
        output_file = OUTPUT_DIR / f"{category['slug']}.json"
        output_file.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        index["categories"].append(
            {
                "name": category["name"],
                "slug": category["slug"],
                "path": category["path"],
                "records": len(records),
                "file": output_file.name,
            }
        )
        print(f"Сохранено {output_file}: {len(records)} объявлений")

    (OUTPUT_DIR / "index.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Готово. Категорий: {len(parsed)}")


if __name__ == "__main__":
    asyncio.run(main())
