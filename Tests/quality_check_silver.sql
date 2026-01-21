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



