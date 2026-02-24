import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import re
import os
import random

BASE_URL = "https://berkat.ru"

CATEGORIES = [
    {"name": "Авто", "path": "/avto"},
    {"name": "Легковые автомобили", "path": "/avto/legkovye-avtomobili"},
    {"name": "Автозапчасти и принадлежности", "path": "/avto/avtozapchasti-i-prinadlezhnosti"},
    {"name": "Грузовики, автобусы, спецтехника", "path": "/avto/gruzoviki-avtobusy-spectehnika"},
    {"name": "Автосервис и услуги", "path": "/avto/avtoservis-i-uslugi"},
    {"name": "Тюнинг", "path": "/avto/tyuning"},
    {"name": "Шины и диски", "path": "/avto/shiny-i-diski"},
    {"name": "Транспортные услуги", "path": "/avto/transportnye-uslugi"},
    {"name": "Мото-транспорт", "path": "/avto/moto-transport"},
    {"name": "Автозвук", "path": "/avto/avtozvuk"},
    {"name": "Недвижимость", "path": "/nedvizhimost"},
    {"name": "Квартира", "path": "/nedvizhimost/kvartira"},
    {"name": "Дом", "path": "/nedvizhimost/dom"},
    {"name": "Земельный участок", "path": "/nedvizhimost/zemelnyi-uchastok"},
    {"name": "Коммерческая недвижимость", "path": "/nedvizhimost/kommercheskaja-nedvizhimost"},
    {"name": "Дача", "path": "/nedvizhimost/dacha"},
    {"name": "Гараж", "path": "/nedvizhimost/garazh"},
    {"name": "Работа", "path": "/rabota"},
    {"name": "Вакансии", "path": "/rabota/vakansii"},
    {"name": "Резюме", "path": "/rabota/rezjume"},
    {"name": "Услуги", "path": "/uslugi"},
    {"name": "Строительство и ремонт", "path": "/uslugi/stroitelstvo-i-remont"},
    {"name": "Бытовые услуги", "path": "/uslugi/bytovye-uslugi"},
    {"name": "Обучение и курсы", "path": "/uslugi/obuchenie-i-kursy"},
    {"name": "Юридические и финансовые услуги", "path": "/uslugi/juridicheskie-i-finansovye-uslugi"},
    {"name": "Красота и здоровье", "path": "/uslugi/krasota-i-zdorove"},
    {"name": "Праздники и мероприятия", "path": "/uslugi/prazdniki-i-meroprijatija"},
    {"name": "Электроника", "path": "/elektronika"},
    {"name": "Мобильные телефоны", "path": "/elektronika/mobilnye-telefony"},
    {"name": "Компьютеры и ноутбуки", "path": "/elektronika/kompjutery-i-noutbuki"},
    {"name": "Планшеты и электронные книги", "path": "/elektronika/planshety-i-elektronnye-knigi"},
    {"name": "Фото и видеокамеры", "path": "/elektronika/foto-i-videokamery"},
    {"name": "Игровые приставки и игры", "path": "/elektronika/igrovye-pristavki-i-igry"},
    {"name": "Оргтехника и расходники", "path": "/elektronika/orgtehnika-i-rashodniki"},
    {"name": "ТВ и видео", "path": "/elektronika/tv-i-video"},
    {"name": "Аудиотехника", "path": "/elektronika/audiotehnika"},
    {"name": "Бытовая техника", "path": "/bytovaja-tehnika"},
    {"name": "Для кухни", "path": "/bytovaja-tehnika/dlja-kuhni"},
    {"name": "Для дома", "path": "/bytovaja-tehnika/dlja-doma"},
    {"name": "Климатическая техника", "path": "/bytovaja-tehnika/klimaticheskaja-tehnika"},
    {"name": "Дом и сад", "path": "/dom-i-sad"},
    {"name": "Мебель и интерьер", "path": "/dom-i-sad/mebel-i-interer"},
    {"name": "Ремонт и строительство", "path": "/dom-i-sad/remont-i-stroitelstvo"},
    {"name": "Посуда и товары для кухни", "path": "/dom-i-sad/posuda-i-tovary-dlja-kuhni"},
    {"name": "Продукты питания", "path": "/dom-i-sad/produkty-pitanija"},
    {"name": "Растения", "path": "/dom-i-sad/rastenija"},
    {"name": "Личные вещи", "path": "/lichnye-veshhi"},
    {"name": "Одежда, обувь, аксессуары", "path": "/lichnye-veshhi/odezhda-obuv-aksessuary"},
    {"name": "Детская одежда и обувь", "path": "/lichnye-veshhi/detskaja-odezhda-i-obuv"},
    {"name": "Товары для детей и игрушки", "path": "/lichnye-veshhi/tovary-dlja-detei-i-igrushki"},
    {"name": "Часы и украшения", "path": "/lichnye-veshhi/chasy-i-ukrashenija"},
    {"name": "Красота и здоровье", "path": "/lichnye-veshhi/krasota-i-zdorove"},
    {"name": "Хобби и отдых", "path": "/hobbi-i-otdyh"},
    {"name": "Спорт и отдых", "path": "/hobbi-i-otdyh/sport-i-otdyh"},
    {"name": "Книги и журналы", "path": "/hobbi-i-otdyh/knigi-i-zhurnaly"},
    {"name": "Коллекционирование", "path": "/hobbi-i-otdyh/kollekcionirovanie"},
    {"name": "Музыкальные инструменты", "path": "/hobbi-i-otdyh/muzykalnye-instrumenty"},
    {"name": "Животные", "path": "/zhivotnye"},
    {"name": "Собаки", "path": "/zhivotnye/sobaki"},
    {"name": "Кошки", "path": "/zhivotnye/koshki"},
    {"name": "Птицы", "path": "/zhivotnye/pticy"},
    {"name": "Сельхозживотные", "path": "/zhivotnye/selhozzhivotnye"},
    {"name": "Товары для животных", "path": "/zhivotnye/tovary-dlja-zhivotnyh"},
    {"name": "Для бизнеса", "path": "/dlja-biznesa"},
    {"name": "Готовый бизнес", "path": "/dlja-biznesa/gotovyi-biznes"},
    {"name": "Оборудование для бизнеса", "path": "/dlja-biznesa/oborudovanie-dlja-biznesa"}
]

class Parser:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(5)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    async def fetch(self, session, url, retries=3):
        for _ in range(retries):
            async with self.semaphore:
                try:
                    async with session.get(url, timeout=30) as r:
                        if r.status == 200:
                            await asyncio.sleep(random.uniform(0.2, 0.5))
                            return await r.text()
                except:
                    await asyncio.sleep(1)
        return None

    async def get_ads(self, session, category_obj, limit=10):
        url = BASE_URL + category_obj["path"]
        ads = []
        page = 1
        current = url
        while current and len(ads) < limit:
            print(f"Категория: {category_obj['name']} | Страница {page} -> {current}")
            html = await self.fetch(session, current)
            if not html:
                break
            soup = BeautifulSoup(html, "html.parser")
            links = soup.select("h3 a[href]")
            for link in links:
                if len(ads) >= limit:
                    break
                
                href = link["href"]
                if href.startswith("/"):
                    full = BASE_URL + href
                elif "berkat.ru" in href:
                    full = href
                else:
                    continue
                ads.append({
                    "title": link.get_text(strip=True),
                    "url": full,
                    "category": category_obj["name"],
                    "category_url": url
                })
            
            if len(ads) < limit:
                next_btn = soup.select_one("a.next_page, a[rel='next']")
                if next_btn and next_btn.get("href"):
                    href = next_btn["href"]
                    current = BASE_URL + href if href.startswith("/") else href
                    page += 1
                else:
                    break
            else:
                break
        return ads

    async def parse_details(self, session, ad):
        html = await self.fetch(session, ad["url"])
        if not html:
            return ad
        soup = BeautifulSoup(html, "html.parser")

        # --- Местоположение (Город) ---
        location = ""
        city_span = soup.find("span", class_="board_item_city")
        if city_span:
            location = city_span.get_text(strip=True)
        ad["location"] = location

        # --- Описание ---
        description = ""
        desc_div = soup.find("div", class_="board_item_description") or soup.find("div", class_="text")
        if desc_div:
            description = desc_div.get_text(strip=True)
        
        if not description:
            info_div = soup.find("div", class_="board_item_info")
            if info_div:
                b_tags = info_div.find_all("b")
                for b in b_tags:
                    txt = b.get_text(strip=True)
                    if len(txt) > 20:
                        description = txt
                        break
        
        if not description:
            view_div = soup.find("div", class_="board_item_view")
            if view_div:
                for div in view_div.find_all("div", recursive=False):
                    txt = div.get_text(strip=True)
                    if len(txt) > 30 and "Дата:" not in txt and "Просмотры:" not in txt:
                        description = txt
                        break
        
        ad["description"] = description

        # --- Фото ---
        photos = []
        fotorama = soup.find("div", class_="fotorama")
        if fotorama:
            for a_tag in fotorama.find_all("a"):
                src = (
                    a_tag.get("data-full")
                    or a_tag.get("href")
                    or (a_tag.find("img") and a_tag.find("img").get("src"))
                    or ""
                )
                if src:
                    if src.startswith("/"):
                        src = BASE_URL + src
                    if src not in photos:
                        photos.append(src)
        ad["photos"] = photos

        # --- Дата ---
        published_at = ""
        date_spans = soup.find_all("span", class_="board_item_date")
        for ds in date_spans:
            text = ds.get_text(strip=True)
            if text.startswith("Дата:"):
                time_tag = ds.find("time")
                if time_tag:
                    published_at = time_tag.get("datetime") or time_tag.get_text(strip=True)
                break
        ad["published_at"] = published_at

        # --- Просмотры ---
        views = 0
        hits_span = soup.find("span", class_="board_item_hits")
        if hits_span:
            m = re.search(r"\d+", hits_span.get_text())
            if m:
                views = int(m.group())
        ad["views"] = views

        return ad

async def main():
    os.makedirs("parsed_data", exist_ok=True)
    parser = Parser()
    async with aiohttp.ClientSession(headers=parser.headers) as session:
        all_ads_brief = []
        print("Парсинг списков объявлений (по 10 на категорию)...")
        for cat in CATEGORIES:
            ads = await parser.get_ads(session, cat, limit=10)
            all_ads_brief.extend(ads)
        
        print(f"Всего найдено объявлений для детального парсинга: {len(all_ads_brief)}")
        
        print("Парсинг деталей объявлений...")
        results = []
        batch_size = 20
        for i in range(0, len(all_ads_brief), batch_size):
            batch = all_ads_brief[i:i + batch_size]
            tasks = [parser.parse_details(session, ad) for ad in batch]
            res = await asyncio.gather(*tasks)
            results.extend(res)
            print(f"Обработано {len(results)} / {len(all_ads_brief)}")
        
        output_file = "parsed_data/berkat_10_per_category.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        
        print(f"ГОТОВО. Результат сохранен в {output_file}")

if __name__ == "__main__":
    asyncio.run(main())
