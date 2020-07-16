-- Count the number of objects in the RAVE DR4 database

-- LANGUAGE = PostgreSQL
-- QUEUE = 60s

SELECT count(*)
FROM "ravedr4"."rave_dr4";
