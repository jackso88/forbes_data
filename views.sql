USE test;

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
       ABS((a.prev_val-a.value)/(a.value+a.prev_val)/2) AS delta_coef, 
       calendar.date, 
       DENSE_RANK() OVER(PARTITION BY calendar.date 
       ORDER BY ABS((a.prev_val-a.value)/(a.value+a.prev_val)/2)) AS place
FROM
    (SELECT country_id,
            CASE WHEN LAG(value) OVER(ORDER BY value) IS NULL 
                 THEN LEAD(value) OVER(ORDER BY value)
                 ELSE LAG(value) OVER(ORDER BY value) END AS prev_val,
            value, 
            date 
	 FROM forbes)a
JOIN calendar ON calendar.date = a.date;
