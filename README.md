# Berkat parser

Парсер объявлений с [berkat.ru](https://berkat.ru), который сохраняет данные по категориям в компактном **постраничном JSON-формате**.

## Зачем нужен постраничный формат

Большой единый JSON неудобен для браузера: `response.json()` сначала скачивает весь файл, а затем разбирает его в памяти. При размере десятков мегабайт сайт начинает долго загружаться или зависать.

Поэтому парсер не создаёт монолитные файлы вроде `transport.json`. Каждая категория разбивается на страницы по **250 объявлений**:

```text
parsed_data/
├── index.json
├── transport/
│   ├── page-0001.json
│   ├── page-0002.json
│   └── ...
├── real-estate/
│   └── page-0001.json
└── jobs/
    └── page-0001.json
```

Браузер загружает только необходимую страницу, а не весь каталог. JSON также записывается без пробелов и отступов, что уменьшает размер файлов. Новые данные сначала собираются во временный каталог и заменяют предыдущий набор только после успешного завершения парсинга.

## Формат `index.json`

Файл `parsed_data/index.json` содержит список категорий, количество объявлений и доступные страницы:

```json
{
  "version": 2,
  "page_size": 250,
  "categories": [
    {
      "slug": "transport",
      "records": 1250,
      "page_size": 250,
      "pages": ["page-0001.json", "page-0002.json"],
      "directory": "transport/"
    }
  ]
}
```

## Использование на сайте

Сначала загрузите индекс, чтобы узнать количество страниц:

```js
const index = await fetch("/parsed_data/index.json").then((response) => {
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
});
```

Загрузка одной страницы категории:

```js
async function loadAds(category, page = 1) {
  const filename = `page-${String(page).padStart(4, "0")}.json`;
  const response = await fetch(`/parsed_data/${category}/${filename}`);

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  return response.json();
}

const ads = await loadAds("transport", 1);
console.log(ads);
```

Для перехода между страницами не объединяйте все ответы в один массив. Храните только текущую страницу в состоянии интерфейса:

```js
const ads = await loadAds("real-estate", currentPage);
renderAds(ads);
```

Для GitHub Raw URL используется такой же путь:

```text
https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/transport/page-0001.json
```

## Структура объявления

Каждый элемент страницы содержит следующие поля:

| Поле | Тип | Описание |
| --- | --- | --- |
| `title` | string | Заголовок объявления |
| `url` | string | Ссылка на исходное объявление |
| `category` | string | Название категории |
| `category_slug` | string | Слаг категории |
| `category_url` | string | Ссылка на раздел сайта |
| `location` | string | Местоположение |
| `description` | string | Описание |
| `price` | string | Цена в исходном формате |
| `photos` | array | Ссылки на фотографии |
| `published_at` | string | Дата публикации |
| `views` | integer | Количество просмотров |

Некоторые поля могут быть пустыми, если исходная страница не содержит значения или разметка сайта изменилась.

## Локальный запуск

```bash
git clone https://github.com/TETRIX8/berkatnew.git
cd berkatnew
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python berkat_parser_v2.py
```

После выполнения новые данные появятся в `parsed_data/`. Размер страницы задаётся константой `PAGE_SIZE` в `berkat_parser_v2.py`; по умолчанию это 250 объявлений.

## Автоматическое обновление

Workflow [`.github/workflows/berkat_workflow.yml`](./.github/workflows/berkat_workflow.yml) запускается каждый час по UTC и может быть запущен вручную через **Actions → Hourly Berkat Parse → Run workflow**.

Во время запуска workflow:

1. устанавливается Python 3.11;
2. устанавливаются зависимости из `requirements.txt`;
3. собираются объявления по категориям;
4. каждая категория разбивается на компактные файлы `page-XXXX.json`;
5. пересоздаётся `index.json`;
6. обновлённый каталог данных коммитится в ветку `main`.

Если парсер завершается с ошибкой, предыдущий каталог `parsed_data/` не удаляется. Это защищает сайт от публикации неполного набора данных.

## Важное ограничение

JSON-файлы подходят для постраничного чтения и умеренной нагрузки. Если сайту понадобится полнотекстовый поиск, сложная фильтрация или высокая параллельная нагрузка, лучше перенести данные в SQLite/PostgreSQL и отдавать их через API с параметрами `page`, `limit`, `category` и `q`.

Парсер не является официальным API Berkat.ru. HTML-разметка исходного сайта может меняться, поэтому при резком падении количества записей или росте пустых полей необходимо проверить URL категорий и CSS-селекторы в `berkat_parser_v2.py`.

## Источник и лицензия

Источник объявлений: [berkat.ru](https://berkat.ru). Перед публикацией или коммерческим использованием данных самостоятельно проверьте применимые условия, права и ограничения.
