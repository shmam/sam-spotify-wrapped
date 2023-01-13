SELECT
    count(*) AS tracks_played, user_name
FROM Activity
GROUP BY user_name
ORDER BY count(*) DESC 