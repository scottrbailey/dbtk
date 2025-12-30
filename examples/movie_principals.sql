SELECT
  sub.nconst,
  MAX(primary_name)                     name,
  MAX(CASE WHEN n.death_year IS NOT NULL
	    THEN 'decd. ' || n.death_year
	    ELSE (EXTRACT(YEAR FROM CURRENT_DATE) - n.birth_year)::text 
	  END)                              current_age,
  string_agg(DISTINCT category, ', ')   roles,
  MAX(array_to_string(n.primary_profession, ', ')) lifetime_professions,
  MAX(CASE WHEN rn = 1 THEN tconst END) movie_1,
  MAX(CASE WHEN rn = 2 THEN tconst END) movie_2,
  MAX(CASE WHEN rn = 3 THEN tconst END) movie_3,
  MAX(CASE WHEN rn = 4 THEN tconst END) movie_4
FROM (
	SELECT DISTINCT ON (p.nconst, p.tconst)
	  p.nconst,
	  p.tconst,
	  p.category,
	  t.primary_title movie,
	  row_number() OVER (PARTITION BY p.nconst ORDER BY t.start_year, t.primary_title, p.ordering) rn
	FROM title_principals_subset p 
	JOIN titles_subset t ON p.tconst = t.tconst
	WHERE %(genre)s = ANY(t.genres)
	  AND (%(excl_roles)s IS NULL 
	    OR NOT (p.category = ANY(%(excl_roles)s)))
	  AND (%(incl_roles)s IS NULL
	    OR p.category = ANY(%(incl_roles)s))
	ORDER BY p.nconst, p.tconst
) sub
JOIN names_subset n ON sub.nconst = n.nconst
GROUP BY sub.nconst
ORDER BY name, nconst