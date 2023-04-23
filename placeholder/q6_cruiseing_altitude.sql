SELECT titles.primary_title,
    ratings.votes
FROM titles
    INNER JOIN ratings USING(title_id)
WHERE title_id IN (
        SELECT DISTINCT(title_id)
        FROM crew
        WHERE person_id IN (
                SELECT person_id
                FROM people
                WHERE name LIKE '%Cruise%'
                    and born = 1962
            )
    )
ORDER BY ratings.votes DESC
LIMIT 10;