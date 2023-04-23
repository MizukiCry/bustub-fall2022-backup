SELECT name, COUNT(*) as num_appearances
FROM crew JOIN people ON crew.person_id = people.person_id
GROUP BY crew.person_id
ORDER BY num_appearances DESC
LIMIT 20;