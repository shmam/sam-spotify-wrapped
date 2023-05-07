-- # total hours so far
SELECT (SUM(item_duration_ms)/1000)/60 AS total_minutes
FROM MyActivity;

-- # top songs by plays
SELECT item_name,
         COUNT(*),
         item_artists_name
FROM MyActivity
GROUP BY  item_name, item_artists_name
ORDER BY  count(*) DESC ;

-- # top artists
SELECT item_artists_name,
         COUNT(*),
         item_artists_name
FROM MyActivity
GROUP BY  item_artists_name
ORDER BY  count(*) DESC ;