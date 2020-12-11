USE test;

-- Creating table with dates list
CREATE TABLE calendar (
    `date` DATE NOT NULL,
    `day` INT,
    `month` INT,
    `year` INT,
    `day_name` NVARCHAR(45),
    `month_name` NVARCHAR(45),
    `week_num` INT,
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
            WEEK(str_date),
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
CALL calendar('2019-01-01', '2022-12-31');

