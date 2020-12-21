USE test;

CREATE VIEW wFluct AS
SELECT country_id, 
       (MAX(value)-MIN(value))/AVG(value) AS delta_coef,
       WEEK(date) AS week_num,
       DENSE_RANK() OVER(PARTITION BY WEEK(date)
       ORDER BY (MAX(value)-MIN(value))/AVG(value)) AS place
FROM forbes
GROUP BY 1, 3;

CREATE VIEW dFluct AS
SELECT country_id, 
       ROUND(ABS((prev_val-value)/(value+prev_val)/2),6) AS delta_coef, 
       date, 
       DENSE_RANK() OVER(PARTITION BY date 
       ORDER BY ABS((prev_val-value)/(value+prev_val)/2)) AS place
FROM
    (SELECT country_id,
            ROUND(LAG(value) OVER(PARTITION BY country_id ORDER BY date),5) AS prev_val,
            value, 
            date 
     FROM forbes)a;

CREATE VIEW cov_cur AS
SELECT country_id, 
       value, 
       forbes.date, 
       a.active_cases 
FROM forbes
LEFT JOIN currencies ON currencies.cur_id = forbes.country_id
LEFT JOIN (SELECT country_name, 
				  date, 
                  confirmed-deaths-recovered AS active_cases 
           FROM covid)a
ON a.country_name = currencies.country AND a.date = forbes.date
