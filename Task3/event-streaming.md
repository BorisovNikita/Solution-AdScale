# Архитектура потоковой обработки событий (Kafka)

## 1. Топики Kafka

| Топик | Producer | Consumer | Описание |
|-------|----------|----------|----------|
| `auction-results` | Сервер ставок | Финансовый сервис, Сервис аналитики | События о завершённых аукционах (победитель, цена, ставка). |
| `user-events` | Сервис выдачи | Сервис статистики | События показов (impression) и кликов (click). |
| `campaign-updates` | Рекламный кабинет / Сервис кампаний | Сервис ставок (инвалидация кэша) | Уведомления об изменении кампаний, объявлений, бюджетов, статусов. |
| `financial-transactions` | Финансовый сервис | Сервис аналитики, внешние системы (аудит) | События о финансовых операциях (пополнения, списания, выставление счетов). |

## 2. Схема событий (JSON)

В каждом событии обязательно присутствуют поля:

- `event_id` – уникальный идентификатор события (UUID).
- `event_type` – тип события (строка, например, `auction_won`, `impression`, `click`).
- `timestamp` – время события в формате ISO 8601 (или Unix timestamp в миллисекундах).
- `version` – версия схемы (целое число, для обратной совместимости).

### Пример - схема события `auction_won` (топик `auction-results`)

```json
{
  "type": "object",
  "properties": {
    "event_id": { "type": "string", "format": "uuid" },
    "event_type": { "enum": ["auction_won"] },
    "timestamp": { "type": "string", "format": "date-time" },
    "version": { "type": "integer", "minimum": 1 },
    "payload": {
      "type": "object",
      "properties": {
        "auction_id": { "type": "string", "format": "uuid" },
        "request_id": { "type": "string", "format": "uuid" },
        "campaign_id": { "type": "string", "format": "uuid" },
        "ad_id": { "type": "string", "format": "uuid" },
        "win_price": { "type": "number", "minimum": 0 },
        "bid_amount": { "type": "number", "minimum": 0 },
        "user_id": { "type": "string" },
        "context": {
          "type": "object",
          "properties": {
            "device": { "type": "string" },
            "geo": { "type": "string" },
            "user_agent": { "type": "string" }
          }
        }
      },
      "required": ["auction_id", "campaign_id", "ad_id", "win_price", "user_id"]
    }
  },
  "required": ["event_id", "event_type", "timestamp", "version", "payload"]
}
```


## 3. Группы потребителей (Consumer Groups)

| Consumer Group | Топики | Назначение | Количество потребителей |
|----------------|--------|------------|-------------------------------------|
| `finance-group` | `auction-results` | Финансовый сервис списывает средства за выигранные аукционы. | 3 |
| `analytics-group` | `auction-results`, `financial-transactions` | Сервис аналитики строит отчёты (агрегации по кампаниям, финансовые сводки). | 2 |
| `stats-group` | `user-events` | Сервис статистики записывает показы и клики в ClickHouse. | 4 |
| `cache-group` | `campaign-updates` | Сервис ставок инвалидирует кэш (Redis) при обновлении кампаний. | 1 |

## 4. Политика хранения

| Топик | Retention (время) | Retention (размер) | Очистка | Причина |
|-------|-------------------|-------------------|---------|---------|
| `auction-results` | 7 дней | 100 ГБ | удаление по времени и размеру | Для аудита и быстрых отчётов. Старые данные архивируются. |
| `user-events` | 7 дней | 500 ГБ | удаление по времени и размеру | Достаточно для агрегаций, после записи в ClickHouse можно удалять. |
| `campaign-updates` | 1 день | 10 ГБ | удаление по времени | Только для инвалидации кэша, долгое хранение не требуется. |
| `financial-transactions` | 30 дней | 50 ГБ | удаление по времени и размеру | Для аудита и сверки с платёжным шлюзом. |