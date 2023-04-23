SELECT DISTINCT(people.name)
FROM crew
    INNER JOIN people USING(person_id)
WHERE crew.title_id IN (
        SELECT title_id
        FROM crew
        WHERE person_id IN (
                SELECT person_id
                FROM people
                WHERE name = 'Nicole Kidman'
                    and born = 1967
            )
    )
    AND (
        crew.category = 'actor'
        OR crew.category = 'actress'
    )
ORDER BY people.name;