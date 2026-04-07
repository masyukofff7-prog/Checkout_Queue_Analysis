-- Пояснение:
-- 1. Первый запрос объединяет таблицы store_checkout_queues и store_stores по store_uuid,
--    фильтрует магазины 98451680 и 12864064 и приводит поля дат к timestamp.
-- 2. Второй запрос определяет периоды очереди на кассах через анализ разрыва
--    между окончанием текущей и началом следующей операции на одной кассе.
-- 3. Если разрыв не превышает 5 секунд, это интерпретируется как признак очереди.
-- 4. Метод является прокси-подходом, так как фактическое время ожидания покупателей
--    в исходных данных отсутствует.


-- =========================================
-- 1. Merge + filter (основная таблица)
-- =========================================

SELECT
    scq.checks_number,
    ss.store_id,
    scq.employees_id,
    scq.quantity,
    scq.selling_price,
    scq.checkout_id1 AS checkout_id,
    to_timestamp(scq.start_operation_dt, 'DD.MM.YYYY HH24:MI:SS') AS start_operation_dt,
    to_timestamp(scq.end_operation_dt, 'DD.MM.YYYY HH24:MI:SS') AS end_operation_dt
FROM store_checkout_queues scq
JOIN store_stores ss
    ON scq.store_uuid = ss.store_uuid
WHERE ss.store_id IN (98451680, 12864064);


-- =========================================
-- 2. Queue periods (расчёт очередей)
-- =========================================

WITH merged AS (
    SELECT
        scq.checks_number,
        ss.store_id,
        scq.employees_id,
        scq.quantity,
        scq.selling_price,
        scq.checkout_id1 AS checkout_id,
        to_timestamp(scq.start_operation_dt, 'DD.MM.YYYY HH24:MI:SS') AS start_operation_dt,
        to_timestamp(scq.end_operation_dt, 'DD.MM.YYYY HH24:MI:SS') AS end_operation_dt
    FROM store_checkout_queues scq
    JOIN store_stores ss
        ON scq.store_uuid = ss.store_uuid
    WHERE ss.store_id IN (98451680, 12864064)
),
ordered AS (
    SELECT
        m.*,
        LEAD(m.start_operation_dt) OVER (
            PARTITION BY m.store_id, m.checkout_id
            ORDER BY m.start_operation_dt
        ) AS next_start_operation_dt
    FROM merged m
),
gaps AS (
    SELECT
        o.*,
        EXTRACT(EPOCH FROM (o.next_start_operation_dt - o.end_operation_dt)) AS gap_to_next_start_s
    FROM ordered o
)
SELECT *
FROM gaps;
