CREATE VIEW wFluct AS
SELECT a.country_id, 
       a.avg AS delta,
       a.week AS week,
       DENSE_RANK() OVER(PARTITION BY a.week ORDER BY a.avg) AS place 
FROM 
    (SELECT country_id, 
            (MAX(value)-AVG(value))+ABS(MIN(value)-AVG(value)) AS avg,
            WEEK(date) AS week
            FROM forbes
            GROUP BY country_id, WEEK(date))a
GROUP BY 1, 3
ORDER BY 3 DESC;

CREATE VIEW dFluct AS
SELECT b.country_id, 
       b.delta, 
       b.date, 
       DENSE_RANK() OVER(PARTITION BY b.date ORDER BY b.delta) AS place
       FROM
       (SELECT forbes.country_id AS country_id, 
               ABS(a.avg-value) AS delta, 
               date FROM forbes,
               (SELECT country_id, 
                       AVG(value) AS avg 
                FROM forbes
                GROUP BY 1)a
        WHERE forbes.country_id = a.country_id)b
