# ANSWERS

´´´
SELECT
    *
FROM
    yellow_taxi_data t,
    taxi_zone zpu,
    taxi_zone zdo
WHERE
    t."PULocationID" = zpu."LocationID" AND
    t."DOLocationID" = zdo."LocationID"
LIMIT 100;
´´´