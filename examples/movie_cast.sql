SELECT
  sub.nconst,
  MAX(name)                  name,
  MAX(current_age)           current_age,
  MAX(CASE WHEN rn = 1 THEN movie END)  movie_1,
  MAX(CASE WHEN rn = 1 THEN sub.tconst END) movie_id_1,
  MAX(CASE WHEN rn = 2 THEN movie END)  movie_2,
  MAX(CASE WHEN rn = 2 THEN sub.tconst END) movie_id_2,
  MAX(CASE WHEN rn = 3 THEN movie END)  movie_3,
  MAX(CASE WHEN rn = 3 THEN sub.tconst END) movie_id_3,
  MAX(CASE WHEN rn = 4 THEN movie END)  movie_4,
  MAX(CASE WHEN rn = 4 THEN sub.tconst END) movie_id_4
FROM (
	SELECT DISTINCT ON (p.nconst, p.tconst)
	  p.nconst,
	  p.tconst,
	  n.primary_name         name,
	  CASE WHEN n.death_year IS NOT NULL
	    THEN 'died ' || n.death_year
	    ELSE (EXTRACT(YEAR FROM CURRENT_DATE) - n.birth_year)::text END current_age,
	  CASE WHEN t.tconst = ANY(n.known_for_titles)
	    THEN t.primary_title ||' *'
		ELSE t.primary_title END movie,
	  t.primary_title,
	  row_number() OVER (PARTITION BY p.nconst ORDER BY t.start_year, t.primary_title, p.ordering) rn
	FROM names_subset n
	JOIN title_principals_subset p ON n.nconst = p.nconst
	JOIN titles_subset t ON p.tconst = t.tconst
	WHERE p.category IN ('actor', 'actress')
	ORDER BY p.nconst, p.tconst 
) sub
JOIN titles_subset t ON sub.tconst = t.tconst
WHERE %(genre)s = ANY(t.genres)
GROUP BY sub.nconst
ORDER BY name, sub.nconst