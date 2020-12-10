CREATE VIEW wFluct AS
SELECT country_id, 
       (MAX(value)-MIN(value))/AVG(value) AS delta_coef,
       calendar.week_num,
       DENSE_RANK() OVER(PARTITION BY calendar.week_num 
       ORDER BY ABS(1-(MAX(value)/AVG(value)))+ABS((MIN(value)/AVG(value))-1)) AS place
       FROM forbes
JOIN calendar ON calendar.date = forbes.date
GROUP BY 1, 3;

CREATE VIEW dFluct AS
SELECT a.country_id, 
       ABS((a.next_val-a.value)/a.value) AS delta_coef, 
       calendar.date, 
       DENSE_RANK() OVER(PARTITION BY calendar.date 
       ORDER BY ABS((a.next_val-a.value)/a.value)) AS place
FROM
	(SELECT country_id,
            LEAD(value) OVER(ORDER BY value) AS next_val,
            value, 
            date FROM forbes)a
JOIN calendar ON calendar.date = a.date;
