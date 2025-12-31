SELECT t.tconst,
  trim(t.primary_title) primary_title,
  t.title_type,
  t.start_year,
  array_to_string(t.genres, ', ') genres,
  t.avg_rating,
  t.num_votes,
  dp.produced_by,
  dp.directed_by
FROM titles_subset t
LEFT JOIN (
  SELECT sub.tconst,
    MAX(CASE WHEN sub.role = 'producer' THEN sub.names END) produced_by,
	MAX(CASE WHEN sub.role = 'director' THEN sub.names END) directed_by
  FROM (
	  SELECT p.tconst,
		p.category role,
		string_agg(n.primary_name, ', ' ORDER BY p.ordering) names,
		COUNT(1) cnt
	  FROM title_principals_subset p
	  JOIN names_subset n ON p.nconst = n.nconst
	  WHERE p.category IN ('director', 'producer')
	  GROUP BY p.tconst, p.category
  ) sub
  GROUP BY tconst
) dp ON t.tconst = dp.tconst
WHERE %(genre)s = ANY(t.genres)
  AND t.start_year BETWEEN 2020 AND 2022
ORDER BY primary_title