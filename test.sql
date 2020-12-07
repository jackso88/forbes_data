USE test;

-- Creating table with dates list
CREATE TABLE dates (
	`date` DATE NOT NULL,
	PRIMARY KEY (`date`)
    );

-- Creating stored procedure for filling table with date
DELIMITER $$
USE `test`$$
CREATE PROCEDURE `list_dates`(str_date date)
BEGIN
DECLARE i INT DEFAULT 0;
	WHILE i < datediff(curdate(), str_date) DO
		INSERT INTO dates VALUES(DATE_ADD(str_date, interval i day));
        SET i = i + 1;
	END WHILE;
END;$$
DELIMITER ;

-- Filling table with dates
SET @str_date = (SELECT DISTINCT date FROM forbes ORDER BY date LIMIT 1);
CALL list_dates(@str_date);

-- Checking data for missing dates
SELECT dates.date FROM dates
LEFT JOIN forbes on forbes.date = dates.date
WHERE forbes.date IS NULL;

-- Clearing dates list
DELETE FROM dates WHERE date <= CURDATE();

-- Сhecking data for deviations
WITH
	avg_rows AS (SELECT AVG(A.cnt) AS a_rows FROM (SELECT COUNT(*) AS cnt 
				                                   FROM forbes
	                                               GROUP BY date) A),
	date_diff AS (SELECT DATEDIFF(CURDATE(), (SELECT date 
										      FROM forbes 
										      ORDER BY 1 LIMIT 1)) AS d_diff),
	total AS (SELECT COUNT(*) AS tot FROM forbes),
    days AS (SELECT COUNT(DISTINCT date) AS day_cnt FROM forbes)
SELECT CASE WHEN tot/d_diff BETWEEN a_rows * 0.9 AND a_rows * 1.1 THEN 'OK' ELSE 'WARNING' END AS rows_cnt,
	   CASE WHEN day_cnt = d_diff THEN 'OK' ELSE 'WARNING' END AS days_cnt
FROM avg_rows, date_diff, total, days t;
