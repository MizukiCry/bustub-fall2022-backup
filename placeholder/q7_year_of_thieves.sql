SELECT COUNT(DISTINCT title_id)
FROM titles
WHERE premiered IN (
        SELECT premiered
        FROM titles
        WHERE primary_title = 'Army of Thieves'
    );