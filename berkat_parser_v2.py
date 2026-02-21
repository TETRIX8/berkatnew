import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import os
import re
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

    {"name": "Телефоны", "path": "/telefony"},
    {"name": "Телефоны (подкатегория)", "path": "/telefony/telefony"},
    {"name": "Сим-номера, паспорт", "path": "/telefony/sim-nomera-passport"},
    {"name": "Аксессуары к телефонам", "path": "/telefony/aksessuary-k-telefonam"},
    {"name": "Ремонт телефонов и запчасти", "path": "/telefony/remont-telefonov-i-zapchasti"},

    {"name": "Строительство и ремонт", "path": "/stroitelstvo-remont"},
    {"name": "Строительные работы", "path": "/stroitelstvo-remont/stroitelnye-raboty"},
    {"name": "Услуги электрика", "path": "/stroitelstvo-remont/uslugi-elektrika"},
    {"name": "Услуги сантехника", "path": "/stroitelstvo-remont/uslugi-santehnika"},
    {"name": "Сантехнические услуги", "path": "/stroitelstvo-remont/santehnicheskie-uslugi"},
    {"name": "Сантехника, газ, отопление", "path": "/stroitelstvo-remont/santehnika-gaz-otoplenie"},
    {"name": "Инструменты, рабочий инвентарь", "path": "/stroitelstvo-remont/instrumenty-rabochii-inventar"},
    {"name": "Строительное оборудование", "path": "/stroitelstvo-remont/stroitelnoe-oborudovanie"},
    {"name": "Строительные и отделочные материалы", "path": "/stroitelstvo-remont/stroitelnye-otdelochnye-materialy"},
    {"name": "Ремонт квартир и офисов", "path": "/stroitelstvo-remont/remont-kvartir-i-ofisov"},
    {"name": "Установка и изготовление на заказ", "path": "/stroitelstvo-remont/ustanovka-i-izgotovlenie-na-zakaz"},
    {"name": "Железо", "path": "/stroitelstvo-remont/zhelezo"},
    {"name": "Песок", "path": "/stroitelstvo-remont/pesok"},
    {"name": "Стекло", "path": "/stroitelstvo-remont/steklo"},
    {"name": "Архитектура и дизайн", "path": "/stroitelstvo-remont/arhitektura-i-dizain"},
    {"name": "Столярные изделия", "path": "/stroitelstvo-remont/stoljarnye-izdelija"},
    {"name": "Прочие услуги по ремонту", "path": "/stroitelstvo-remont/prochie-uslugi-po-remontu"},

    {"name": "Работа", "path": "/rabota"},
    {"name": "Вакансии", "path": "/rabota/vakansii"},
    {"name": "Ищу работу", "path": "/rabota/ischu-rabotu"},

    {"name": "Ислам", "path": "/islam"},

    {"name": "Услуги", "path": "/uslugi"},
    {"name": "Репетиторство", "path": "/uslugi/repetitorstvo"},
    {"name": "Обучение, курсы", "path": "/uslugi/obuchenie-kursy"},
    {"name": "Реклама, оформление", "path": "/uslugi/reklama-oformlenie"},
    {"name": "Пошив одежды", "path": "/uslugi/poshiv-odezhdy"},
    {"name": "Праздники, мероприятия", "path": "/uslugi/prazdniki-meroprijatija"},
    {"name": "Охрана, сыскные услуги", "path": "/uslugi/ohrana-sysknye-uslugi"},
    {"name": "Интернет, программы, сети", "path": "/uslugi/internet-programmy-seti"},
    {"name": "Юридические услуги", "path": "/uslugi/yuridicheskie-uslugi"},
    {"name": "Финансы и аудит", "path": "/uslugi/finansy-i-audit"},
    {"name": "Домашний персонал", "path": "/uslugi/domashnii-personal"},
    {"name": "Прочие услуги", "path": "/uslugi/prochie"},

    {"name": "Компьютер", "path": "/kompyuter"},
    {"name": "Настольные ПК", "path": "/kompyuter/nastolnye-pk"},
    {"name": "Ноутбуки", "path": "/kompyuter/noutbuki"},
    {"name": "Принтеры и картриджи", "path": "/kompyuter/printery-i-kartridzhi"},
    {"name": "Планшетные компьютеры и КПК", "path": "/kompyuter/planshetnye-kompyutery-i-kpk"},
    {"name": "Комплектующие", "path": "/kompyuter/komplektuyuschie"},
    {"name": "Серверы и сети", "path": "/kompyuter/servery-i-seti"},
    {"name": "Игровые приставки", "path": "/kompyuter/igrovye-pristavki"},
    {"name": "Мониторы и ИБП (UPS)", "path": "/kompyuter/monitory-i-ibp-ups"},
    {"name": "Диски, программы, фильмы", "path": "/kompyuter/diski-programmy-filmy"},
    {"name": "Аккаунты", "path": "/kompyuter/akkaunty"},
    {"name": "Компьютерные аксессуары", "path": "/kompyuter/kompyuternye-aksessuary"},
    {"name": "Сканеры", "path": "/kompyuter/skanery"},
    {"name": "МФУ", "path": "/kompyuter/mfu"},

    {"name": "Бытовая электроника", "path": "/bytovaja-elektronika"},
    {"name": "Телевизоры", "path": "/bytovaja-elektronika/televizory"},
    {"name": "Спутниковые системы", "path": "/bytovaja-elektronika/sputnikovye-sistemy"},
    {"name": "Ремонт электроники", "path": "/bytovaja-elektronika/remont"},
    {"name": "Кондиционеры", "path": "/bytovaja-elektronika/kondicionery"},
    {"name": "Видеонаблюдение", "path": "/bytovaja-elektronika/videonablyudenie"},
    {"name": "Сабвуферы, колонки, усилители", "path": "/bytovaja-elektronika/sabvufery-kolonki-usiliteli"},
    {"name": "Видеокамеры", "path": "/bytovaja-elektronika/videokamery"},
    {"name": "Микроволновые печи", "path": "/bytovaja-elektronika/mikrovolnovye-pechi"},
    {"name": "Холодильники", "path": "/bytovaja-elektronika/holodilniki"},
    {"name": "Пылесосы", "path": "/bytovaja-elektronika/pylesosy"},
    {"name": "Техника для дома", "path": "/bytovaja-elektronika/tehnika-dlja-doma"},
    {"name": "Электроника для детей", "path": "/bytovaja-elektronika/elektronika-dlja-detei"},

    {"name": "Бизнес", "path": "/biznes"},
    {"name": "Продажа бизнеса", "path": "/biznes/prodazha-biznesa"},
    {"name": "Оборудование для бизнеса", "path": "/biznes/oborudovanie-dlja-biznesa"},
    {"name": "Деловое партнерство", "path": "/biznes/delovoe-partnerstvo"},
    {"name": "Бизнес-образование", "path": "/biznes/biznes-obrazovanie"},
    {"name": "Сетевой маркетинг", "path": "/biznes/setevoi-marketing"},
    {"name": "Идеи для бизнеса", "path": "/biznes/idei-dlja-biznesa"},

    {"name": "Мебель", "path": "/mebel"},
    {"name": "Для зала", "path": "/mebel/dlja-zala"},
    {"name": "Для кухни", "path": "/mebel/dlja-kuhni"},
    {"name": "Для спальни", "path": "/mebel/dlja-spalnoi"},
    {"name": "Для ванной", "path": "/mebel/dlja-vannoi"},
    {"name": "Детская мебель", "path": "/mebel/detskaja"},
    {"name": "Офисная мебель", "path": "/mebel/ofisnaja"},
    {"name": "Интерьер", "path": "/mebel/interer"},
    {"name": "Разная мебель", "path": "/mebel/raznaja-mebel"},
    {"name": "Сборка мебели", "path": "/mebel/sborka-mebeli"},
    {"name": "Ремонт мебели", "path": "/mebel/remont-mebeli"},

    {"name": "Продовольствие", "path": "/prodovolstvie"},

    {"name": "Спорт, отдых, хобби", "path": "/sport-otdyh-hobbi"},
    {"name": "Велосипеды", "path": "/sport-otdyh-hobbi/velosipedy"},
    {"name": "Туризм, экскурсии, туры", "path": "/sport-otdyh-hobbi/turizm-ekskursii-tury"},
    {"name": "Праздники (хобби)", "path": "/sport-otdyh-hobbi/prazdniki"},
    {"name": "Книги и печатная продукция", "path": "/sport-otdyh-hobbi/knigi-i-pechatnaja-produkcija"},
    {"name": "Музыкальные инструменты", "path": "/sport-otdyh-hobbi/muzykalnye-instrumenty"},
    {"name": "Спортивные секции и клубы", "path": "/sport-otdyh-hobbi/sportivnye-sekcii-i-kluby"},
    {"name": "Спортивная одежда", "path": "/sport-otdyh-hobbi/sportivnaja-odezhda"},
    {"name": "Тренажеры, снаряды", "path": "/sport-otdyh-hobbi/trenazhery-snarjady"},
    {"name": "Спортивное питание", "path": "/sport-otdyh-hobbi/sportivnoe-pitanie"},
    {"name": "Экстремальный спорт", "path": "/sport-otdyh-hobbi/ekstremalnyi-sport"},

    {"name": "Охота и рыбалка", "path": "/ohota-i-rybalka"},
    {"name": "Снаряжение, спецодежда", "path": "/ohota-i-rybalka/snarjazhenie-specodezhda"},
    {"name": "Рыболовные принадлежности", "path": "/ohota-i-rybalka/rybolovnye-prinadlezhnosti"},
    {"name": "Оптика, бинокли", "path": "/ohota-i-rybalka/optika-binokli"},
    {"name": "Ножи", "path": "/ohota-i-rybalka/nozhi"},
    {"name": "Сейфы", "path": "/ohota-i-rybalka/seify"},

    {"name": "Личные вещи", "path": "/lichnye-veschi"},
    {"name": "Одежда", "path": "/lichnye-veschi/odezhda"},
    {"name": "Детям", "path": "/lichnye-veschi/detjam"},
    {"name": "Сувениры, подарки, цветы", "path": "/lichnye-veschi/suveniry-podarki-cvety"},
    {"name": "Антиквариат, ювелирные изделия", "path": "/lichnye-veschi/antikvar-yuvelir"},
    {"name": "Сувениры, подарки", "path": "/lichnye-veschi/suveniry-podarki"},
    {"name": "Часы", "path": "/lichnye-veschi/chasy"},
    {"name": "Посуда", "path": "/lichnye-veschi/posuda"},
    {"name": "Цветы", "path": "/lichnye-veschi/cvety"},

    {"name": "Сельское хозяйство", "path": "/selskoe-hozjaistvo"},
    {"name": "Животные", "path": "/selskoe-hozjaistvo/zhivotnye"},
    {"name": "Птицы", "path": "/selskoe-hozjaistvo/pticy"},
    {"name": "Корма", "path": "/selskoe-hozjaistvo/korma"},
    {"name": "Растения", "path": "/selskoe-hozjaistvo/rastenija"},
    {"name": "Пчелы", "path": "/selskoe-hozjaistvo/pchely"},
    {"name": "Оборудование (с/х)", "path": "/selskoe-hozjaistvo/oborudovanie"},

    {"name": "Медицина, красота", "path": "/medicina-krasota"},
    {"name": "Поиск людей, встречи", "path": "/poisk-lyudei-vstrechi"},
    {"name": "Бесплатно", "path": "/besplatno"},
    {"name": "Разное", "path": "/raznoe"},
]


class Parser:
    def __init__(self, concurrency=15):
        self.semaphore = asyncio.Semaphore(concurrency)
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

        # --- Описание ---
        desc = soup.find("div", class_="text")
        ad["description"] = desc.get_text(strip=True) if desc else ""

        # --- Фото только из галереи объявления (блок fotorama) ---
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

        # --- Дата публикации ---
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

        # --- Количество просмотров ---
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
