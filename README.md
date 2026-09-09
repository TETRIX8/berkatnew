# Berkat.ru Parsed Data API

Репозиторий содержит автоматически собранные объявления с [Berkat.ru](https://berkat.ru), разбитые по отдельным тематическим разделам. GitHub Actions обновляет данные каждый час и сохраняет JSON-файлы в каталоге [`parsed_data/`](./parsed_data/).

> Данные предназначены для справочного и исследовательского использования. Соблюдайте правила и условия исходного сайта, учитывайте ограничения частоты запросов и не используйте данные во вред пользователям.

## Быстрый доступ

Индекс всех доступных разделов:

- [Открыть index.json](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/index.json)
- [Посмотреть JSON в репозитории](https://github.com/TETRIX8/berkatnew/tree/main/parsed_data)

Каждый раздел является самостоятельным JSON-массивом объявлений. Формат raw-ссылки:

```text
https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/{slug}.json
```

Например, транспорт:

```text
https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/transport.json
```

## Разделы и raw-ссылки

| Раздел | Файл | Raw JSON |
|---|---|---|
| Транспорт | `transport.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/transport.json) |
| Недвижимость | `real-estate.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/real-estate.json) |
| Работа | `jobs.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/jobs.json) |
| Услуги | `services.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/services.json) |
| Электроника | `electronics.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/electronics.json) |
| Бытовая техника | `appliances.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/appliances.json) |
| Дом и сад | `home-and-garden.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/home-and-garden.json) |
| Личные вещи | `personal-items.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/personal-items.json) |
| Хобби и отдых | `hobbies-and-leisure.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/hobbies-and-leisure.json) |
| Животные | `animals.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/animals.json) |
| Для бизнеса | `business.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/business.json) |
| Сельхозпродукция | `agriculture.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/agriculture.json) |
| Разное | `other.json` | [raw](https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/other.json) |

## Структура `index.json`

`index.json` содержит дату последнего обновления, источник и список разделов с количеством записей:

```json
{
  "name": "Berkat.ru parsed data",
  "updated_at": "2026-09-09T00:00:00+00:00",
  "source": "https://berkat.ru",
  "categories": [
    {
      "name": "Транспорт",
      "slug": "transport",
      "path": "/avto",
      "records": 1234,
      "file": "transport.json"
    }
  ]
}
```

## Формат объявления

Каждый JSON-файл содержит массив объектов. Поля объекта:

| Поле | Тип | Описание |
|---|---|---|
| `title` | string | Заголовок объявления |
| `url` | string | Полная ссылка на исходное объявление |
| `category` | string | Название раздела на русском |
| `category_slug` | string | Стабильный slug файла |
| `category_url` | string | Ссылка на страницу раздела |
| `location` | string | Город или местоположение |
| `description` | string | Описание объявления |
| `price` | string | Цена в исходном формате |
| `photos` | array | Ссылки на фотографии |
| `published_at` | string | Дата публикации в исходном формате |
| `views` | integer | Количество просмотров |

Некоторые поля могут быть пустыми, если исходная страница не содержит значения или изменилась разметка сайта.

## Использование как API

GitHub Raw можно использовать как простой read-only JSON API без отдельного сервера.

### `curl`

```bash
curl -L \
  https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/transport.json
```

Получить индекс:

```bash
curl -L \
  https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/index.json
```

### JavaScript

```js
const url = "https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/jobs.json";
const ads = await fetch(url).then((response) => {
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
});

console.log(`Объявлений: ${ads.length}`);
console.log(ads.slice(0, 10));
```

### Python

```python
import requests

url = "https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/real-estate.json"
response = requests.get(url, timeout=30)
response.raise_for_status()
ads = response.json()

for ad in ads[:10]:
    print(ad["title"], ad["url"])
```

### Фильтрация через `jq`

Первые объявления с заполненной ценой:

```bash
curl -sL \
  https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/transport.json \
  | jq '[.[] | select(.price != "")] | .[:10]'
```

Объявления из конкретного города:

```bash
curl -sL \
  https://raw.githubusercontent.com/TETRIX8/berkatnew/main/parsed_data/real-estate.json \
  | jq '[.[] | select(.location | test("Москва"; "i"))]'
```

## Автоматическое обновление

Workflow [`.github/workflows/berkat_workflow.yml`](./.github/workflows/berkat_workflow.yml) запускается каждый час в начале часа по UTC. Также его можно запустить вручную через вкладку **Actions → Hourly Berkat Parse → Run workflow**.

Во время запуска workflow:

1. устанавливает Python 3.11;
2. устанавливает зависимости из `requirements.txt`;
3. собирает объявления по всем разделам;
4. сохраняет каждый раздел в отдельный файл `{slug}.json`;
5. пересоздаёт `index.json`;
6. коммитит изменившиеся JSON-файлы в ветку `main`.

Старые JSON-файлы удаляются перед генерацией, поэтому устаревший монолитный `berkat_full_data.json` и старый `berkat_10_per_category.json` больше не используются.

## Запуск локально

```bash
git clone https://github.com/TETRIX8/berkatnew.git
cd berkatnew
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python berkat_parser_v2.py
```

После выполнения новые файлы появятся в `parsed_data/`.

## Ограничения и рекомендации

Raw-ссылки GitHub подходят для небольших и умеренных объёмов чтения. Для частого или массового использования следует добавить собственное кэширование и соблюдать лимиты GitHub. Не рассчитывайте на мгновенную актуальность: обновление происходит по расписанию, а время появления нового коммита зависит от длительности парсинга.

Парсер не является официальным API Berkat.ru. Структура HTML исходного сайта может меняться, поэтому при резком падении количества записей или росте пустых полей необходимо проверить CSS-селекторы в `berkat_parser_v2.py`.

## Лицензия и источник

В репозитории хранится результат автоматического сбора данных. Источник объявлений: [berkat.ru](https://berkat.ru). Перед публикацией или коммерческим использованием данных самостоятельно проверьте применимые условия, права и ограничения.
