/*
================================================================================
Quality Checks
================================================================================
Script Purpose :
  -This script is to perform various quality checks for data consistency,accuracy,
   and standardization across the 'silver' schemas. It includes checks for:
   - Null or duplicate primary keys.
   - Data standardization and consistency.
   - Unwanted spaces in string fields.
   - Invalid date ranges and orders.
   - Data consistency between related tables.
Usage notes:
   - Run these checks after the loading Silver Layer data.
   - Investigate and resolve for any discrepancies found during checking.
================================================================================
*/

-- ===============================================================
-- Checking 'silver.crm_cust_info'
-- ===============================================================
-- Checks for Nulls and duplicate data 
-- Expectation: none
SELECT
  cst_info,
  COUNT(*)
FROM silver.crm_cust_info
HAVING COUNT(*) > 1 OR cst_info IS NULL;

-- Checks for Data Standardization and Consistency  
-- Expectation: none

SELECT DISTINCT
  cst_marital_status,
  cst_gndr
FROM silver.crm_cust_info;

-- Check for Unwanted spaces in string fields.
-- Expectation: none
SELECT 
  cst_firstname,
  cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 
OR cst_lastname != TRIM(cst_lastname);

-- ===============================================================
-- Checking 'silver.crm_prd_info'
-- ===============================================================
-- Check for Nulls or Duplicate in Primary Key (prd_id)
-- Expectation : None
SELECT 
prd_id,
COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: None
SELECT
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for Null or Negative values
-- Expectation: None
SELECT
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistensy
-- Expectation: None
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ===============================================================
-- Checking 'silver.crm_sales_details'
-- ===============================================================
-- Check for invalid date orders
-- Expectation: None
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt;

-- Check for Data Consistency, Nulls : Between sls_sales , sls_quantity, sls_price
-- >> Sales = price * quantity
-- >> Values must not be nulls, zero or negative
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR  sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 



