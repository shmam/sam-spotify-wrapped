SELECT DISTINCT track_uri as all_distinct_trackuri from
    (
        SELECT distinct track_uri from Activity
        union
        SELECT distinct item_uri from MyActivity
    ) t