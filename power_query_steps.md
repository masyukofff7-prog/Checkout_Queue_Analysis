1) Query MergedChecks: объединённая таблица (требуемые выходные столбцы)

Требуемые столбцы результата:

checks_number — номер чека
store_id — идентификатор магазина
employees_id — идентификатор сотрудника
quantity — количество
selling_price — цена продажи
checkout_id — идентификатор кассы
start_operation_dt — дата и время начала операции
end_operation_dt — дата и время окончания операции

Основные используемые функции (с официальной документацией):

Table.TransformColumnTypes — для задания типов данных (поддерживает указание культуры / locale)
DateTime.FromText — для преобразования текстовых дат/времени с использованием Format и Culture
Table.NestedJoin — для объединения таблиц по ключам
Table.ExpandTableColumn — для извлечения store_id из вложенной таблицы
Table.SelectRows — для фильтрации магазинов

2) Query QueuePeriods: Периоды очередей по каждой кассе (рекомендуемый прокси-подход)
Логика

В рамках каждой пары (store_id, checkout_id):

Отсортировать данные по start_operation_dt
- Table.Sort сортирует по одному или нескольким столбцам
Вычислить next_start_operation_dt (время начала следующей операции)
Рассчитать разрыв до следующего начала:
gap_to_next_start_s = Duration.TotalSeconds(next_start - current_end)
- Duration.TotalSeconds возвращает длительность в секундах
Определить флаг очереди:
queue_flag = gap_to_next_start_s <= ThresholdSeconds
Объединить подряд идущие значения queue_flag в группы
- так называемый подход “gaps and islands” (разрывы и острова)
Агрегация каждой “очередной группы” (island)

Для каждой группы очереди рассчитывается:

queue_start_dt — минимальное время начала
queue_end_dt — максимальное время окончания
checks_with_queue — количество чеков (строк)
queue_duration_s — длительность очереди в секундах:
Duration.TotalSeconds(queue_end_dt - queue_start_dt)
Используемые функции
Table.Group — для группировки
Table.AddIndexColumn — для добавления индекса
List.Accumulate — для накопления (running total / логика последовательностей)
Table.Combine — для объединения результатов групп
