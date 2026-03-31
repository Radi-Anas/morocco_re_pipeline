-- sql/analysis.sql
-- Run these in pgAdmin, DBeaver, or psql after the pipeline loads data.

-- 1. How many listings per city?
SELECT city, COUNT(*) AS total_listings
FROM listings
GROUP BY city
ORDER BY total_listings DESC;

-- 2. Average price and price per m² by city
SELECT
    city,
    ROUND(AVG(price), 0)        AS avg_price,
    ROUND(AVG(price_per_m2), 0) AS avg_price_per_m2,
    COUNT(*)                    AS listings_count
FROM listings
GROUP BY city
ORDER BY avg_price DESC;

-- 3. Most expensive listing per city
SELECT DISTINCT ON (city)
    city, title, price, surface_m2, type
FROM listings
ORDER BY city, price DESC;

-- 4. Property type breakdown
SELECT
    type,
    COUNT(*)           AS count,
    ROUND(AVG(price))  AS avg_price
FROM listings
GROUP BY type
ORDER BY count DESC;

-- 5. Listings added per month (trend over time)
SELECT
    DATE_TRUNC('month', listing_date) AS month,
    COUNT(*) AS new_listings
FROM listings
GROUP BY month
ORDER BY month;

-- 6. Top 5 most affordable listings (price per m²)
SELECT title, city, price, surface_m2, price_per_m2
FROM listings
ORDER BY price_per_m2 ASC
LIMIT 5;