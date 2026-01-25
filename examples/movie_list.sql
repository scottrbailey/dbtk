SELECT t.tconst,
  trim(t.primary_title) "Primary Title",
  t.title_type          "Type",
  t.start_year          "Start Year",
  array_to_string(t.genres, ', ') "Genres",
  t.avg_rating          "Avg Rating",
  t.num_votes           "Num Votes",
  dp.produced_by        "Produced By",
  dp.directed_by        "Directed By"
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
ORDER BY "Primary Title"