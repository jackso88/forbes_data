USE test;

-- Creating table with dates list
CREATE TABLE calendar (
	`date` DATE NOT NULL,
    `day` INT,
    `month` INT,
    `year` INT,
    `day_name` NVARCHAR(45),
    `month_name` NVARCHAR(45),
    `decade` INT,
    `quarter` INT,
    `weekend` NVARCHAR(45),
	PRIMARY KEY (`date`)
    );

-- Creating stored procedure for filling table with date
DELIMITER $$
USE `test`$$
CREATE PROCEDURE `calendar`(str_date date, end_date date)
BEGIN
	WHILE str_date <= end_date DO
		IF str_date NOT IN (SELECT date FROM calendar) THEN
		INSERT INTO calendar VALUES(str_date, 
        DAYOFMONTH(str_date),
        MONTH(str_date), 
        YEAR(str_date), 
        DAYNAME(str_date),
        MONTHNAME(str_date), 
        IF(DAYOFMONTH(str_date)/10 < 1, 1, 
			IF(DAYOFMONTH(str_date)/10 >= 1 AND DAYOFMONTH(str_date)/10 <2, 2, 3)),
        IF(MONTH(str_date)/3 < 1, 1, 
			IF(MONTH(str_date)/3 >= 1 AND MONTH(str_date)/3 < 2, 2, 
				IF(MONTH(str_date)/3 >= 2 AND MONTH(str_date)/3 < 3, 3, 4))),
        IF(DAYNAME(str_date) IN ('SUNDAY', 'SATURDAY'), 'YES', 'NO'));
        END IF;
        SET str_date = DATE_ADD(str_date, INTERVAL 1 DAY);
	END WHILE;
END;$$
DELIMITER ;
 
-- Filling table with dates
CALL calendar('2018-12-01', '2023-01-02');

-- Checking data for missing dates
SELECT calendar.date FROM calendar
LEFT JOIN forbes on forbes.date = calendar.date
WHERE forbes.date IS NULL AND calendar.date < CURDATE()
AND calendar.date >= (SELECT MIN(date) FROM forbes);

-- Clearing dates list
DELETE FROM calendar WHERE date <= CURDATE();

-- Сhecking data for deviations
WITH
	avg_rows AS (SELECT AVG(A.cnt) AS a_rows FROM (SELECT COUNT(*) AS cnt 
				                                   FROM forbes
	                                               GROUP BY date ORDER BY date DESC LIMIT 30) A),
	date_diff AS (SELECT DATEDIFF(CURDATE(), (SELECT date 
										      FROM forbes 
										      ORDER BY 1 LIMIT 1)) AS d_diff),
	total AS (SELECT COUNT(*) AS tot FROM forbes),
    days AS (SELECT COUNT(DISTINCT date) AS day_cnt FROM forbes)
SELECT CASE WHEN tot/d_diff BETWEEN a_rows * 0.9 AND a_rows * 1.1 THEN 'OK' ELSE 'WARNING' END AS rows_cnt,
	   CASE WHEN day_cnt = d_diff THEN 'OK' ELSE 'WARNING' END AS days_cnt
FROM avg_rows, date_diff, total, days t;









