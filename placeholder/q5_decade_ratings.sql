SELECT
    CAST(titles.premiered / 10 * 10 AS CHAR) || 's' AS decade,
    ROUND(AVG(ratings.rating), 2) AS avg_rating,
    ROUND(MAX(ratings.rating), 2),
    ROUND(MIN(ratings.rating), 2),
    COUNT(*)
FROM ratings INNER JOIN titles USING(title_id)
WHERE titles.premiered IS NOT NULL
GROUP BY decade
ORDER BY avg_rating DESC, decade;