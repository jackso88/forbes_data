-- Checking data for missing dates
SELECT calendar.date FROM calendar
LEFT JOIN forbes on forbes.date = calendar.date
WHERE forbes.date IS NULL AND calendar.date < CURDATE()
AND calendar.date >= (SELECT MIN(date) FROM forbes);

-- Сhecking data for deviations
WITH
    avg_rows AS (SELECT AVG(A.cnt) AS a_rows FROM (SELECT COUNT(*) AS cnt FROM forbes
                                                   GROUP BY date ORDER BY date DESC LIMIT 30) A),
    date_diff AS (SELECT DATEDIFF(CURDATE(), (SELECT date FROM forbes ORDER BY 1 LIMIT 1)) AS d_diff),
    total AS (SELECT COUNT(*) AS tot FROM forbes),
    days AS (SELECT COUNT(DISTINCT date) AS day_cnt FROM forbes)
SELECT CASE WHEN tot/d_diff BETWEEN a_rows * 0.9 AND a_rows * 1.1 THEN 'OK' ELSE 'WARNING' END AS rows_cnt,
       CASE WHEN day_cnt = d_diff THEN 'OK' ELSE 'WARNING' END AS days_cnt
FROM avg_rows, date_diff, total, days t;
