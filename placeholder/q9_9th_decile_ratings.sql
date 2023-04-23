WITH actor_avg_rating AS (
    SELECT name,
        person_id,
        ROUND(AVG(rating), 2) as avg_rating
    FROM people
        INNER JOIN crew USING(person_id)
        INNER JOIN titles USING(title_id)
        INNER JOIN ratings USING(title_id)
    WHERE born = 1955
        AND titles.type = 'movie'
    GROUP BY person_id
)
SELECT name,
    avg_rating
FROM (
        SELECT *,
            NTILE(10) OVER(
                ORDER BY avg_rating
            ) AS quartile
        FROM actor_avg_rating
    )
WHERE quartile = 9
ORDER BY avg_rating DESC,
    name;