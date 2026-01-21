/*
===========================================================================================
Stored Procedure: Load Silver Layer (bronze -> silver)
===========================================================================================
Script Purpose:
	This stored procedures loads data from 'bronze' table into the 'silver' schemas that have been performed ETL
	(Extract, Transform, Load) process.
	It perform following actions: 
	- Truncates the 'silver' tables before loading the data.
	- nserts transformed and clean data from Bronze into Silver tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC silver.load_silver;

============================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_batch DATETIME, @end_time_batch DATETIME
	
	BEGIN TRY
		SET @start_time_batch = GETDATE();

		PRINT '===================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '===================================================================';

		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading CRM Table';
		PRINT '-------------------------------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>>>Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status, -- Normalize marital status to readable format
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a' 
			END cst_gndr,  -- Normalize gender to readable format
			cst_create_date
		FROM (
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE flag_last = 1; -- Selecting the most recent record per customer
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>>> Inserting Data Into: silver.crm_prd_info'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status, -- Normalize marital status to readable format
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a' 
			END cst_gndr,  -- Normalize gender to readable format
			cst_create_date
		FROM (
		SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t
		WHERE flag_last = 1; -- Selecting the most recent record per customer
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>>> Inserting Data Into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 
				THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,

		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 
				THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,

		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 
				THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
		END AS sls_sales,
		sls_quantity,

		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0) 
				ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------';

		PRINT '>>-----------------------------------------------------------------';
		PRINT '>>Loading ERP Table';
		PRINT '>>-----------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>>> Inserting Data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
			 ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate -- Set future bdate to NULL
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen -- Normalize gender to readable format and handle unknown values
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>>> Inserting Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 
		(cid, cntry)
		SELECT 
		REPLACE(cid, '-', '') AS cid, -- Remove '-' in the data
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry -- Normalize and handle missing and blank country codes
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>>> Inserting Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2  
		(id,cat,subcat,maintenance)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------------------------------------------------------------';

		SET @end_time_batch = GETDATE()
		PRINT '===================================================================';
		PRINT ' Silver Layer Loads Completed';
		PRINT '- Total Load Duration:' + CAST(DATEDIFF(second, @start_time_batch, @end_time_batch) AS NVARCHAR) + ' seconds';
		PRINT '===================================================================';

	END TRY

	BEGIN CATCH
		PRINT '===================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================================';
	END CATCH
END

