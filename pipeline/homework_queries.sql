-- Homework SQL Queries for Module 1: Docker & SQL
-- Using the green_trips and zones tables loaded via the pipeline

-- Question 3: Counting short trips
-- How many trips had a trip_distance of less than or equal to 1 mile?
SELECT COUNT(*) 
FROM green_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01' 
  AND trip_distance <= 1;
-- Answer: 8007

-- Question 4: Longest trip for each day
-- Which was the pick up day with the longest trip distance?
-- Only consider trips with trip_distance < 100 miles
SELECT DATE(lpep_pickup_datetime) as pickup_date, trip_distance
FROM green_trips 
WHERE trip_distance < 100 
  AND lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
ORDER BY trip_distance DESC 
LIMIT 1;
-- Answer: 2025-11-14

-- Question 5: Biggest pickup zone
-- Which was the pickup zone with the largest total_amount on November 18th, 2025?
SELECT z."Zone", SUM(g."total_amount") as total_sum
FROM green_trips g 
JOIN zones z ON g."PULocationID" = z."LocationID"
WHERE DATE(g."lpep_pickup_datetime") = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total_sum DESC 
LIMIT 1;
-- Answer: East Harlem North

-- Question 6: Largest tip
-- For passengers picked up in "East Harlem North" in November 2025,
-- which was the drop off zone that had the largest tip?
SELECT z."Zone" as dropoff_zone, g."tip_amount"
FROM green_trips g 
JOIN zones z ON g."DOLocationID" = z."LocationID"
WHERE g."PULocationID" IN (
    SELECT "LocationID" FROM zones WHERE "Zone" = 'East Harlem North'
)
  AND g."lpep_pickup_datetime" >= '2025-11-01' 
  AND g."lpep_pickup_datetime" < '2025-12-01'
ORDER BY g."tip_amount" DESC 
LIMIT 1;
-- Answer: Yorkville West

-- Additional verification queries
-- Count total trips in November 2025
SELECT COUNT(*) FROM green_trips 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01';

-- List of zones for reference
SELECT "Zone" FROM zones WHERE "Zone" IN (
  'East Harlem North', 'East Harlem South', 'Morningside Heights', 
  'Forest Hills', 'Yorkville West', 'JFK Airport', 'LaGuardia Airport'
);
