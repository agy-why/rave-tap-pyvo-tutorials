-- Get objects with radial velocites within a range (DR4)

-- LANGUAGE = PostgreSQL
-- QUEUE = 60s

SELECT * 
FROM ravedr4.rave_dr4
  WHERE hrv 
    BETWEEN 5.0 AND 25.0 
  ORDER BY hrv DESC
