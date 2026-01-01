/*
=============================================================
Quality Checks – Gold Layer
=============================================================

Script Purpose:
    This script performs quality checks to validate the
    integrity, consistency, and accuracy of the Gold layer.

    These checks ensure:
    - Uniqueness of surrogate keys in dimension tables
    - Referential integrity between fact and dimension tables
    - Validation of relationships in the star schema
      for analytical purposes

Usage Notes:
    - Run these checks after loading the Silver and Gold layers
    - All queries are expected to return ZERO rows
    - Any returned rows indicate data quality issues
      that must be investigated and resolved
=============================================================
*/

-------------------------------------------------------------
-- Checking gold.dim_customers
-------------------------------------------------------------
-- Check uniqueness of customer_key
-- Expectation: No results
-------------------------------------------------------------
SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-------------------------------------------------------------
-- Checking gold.dim_products
-------------------------------------------------------------
-- Check uniqueness of product_key
-- Expectation: No results
-------------------------------------------------------------
SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-------------------------------------------------------------
-- Checking gold.fact_sales
-------------------------------------------------------------
-- Validate fact-to-dimension relationships
-- Expectation: No orphan records
-------------------------------------------------------------
SELECT
    f.order_number,
    f.customer_key,
    f.product_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE c.customer_key IS NULL
   OR p.product_key IS NULL;
