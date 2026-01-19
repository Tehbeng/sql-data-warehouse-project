/*
===========================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===========================================================================================
Script Purpose:
	This stored procedures loads data from CSV files sources into the 'bronze' schemas.
	It perform following actions: 
	- Truncates the bronze tables before loading the data.
	- Uses the 'BULK INSERT' to load data from CSV Files into 'bronze' tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC broze.load_bronze;

============================================================================================


*/

-- Store procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch DATETIME, @end_batch DATETIME; -- Declare variable to show duration of loading data
	BEGIN TRY
		SET @start_batch = GETDATE();

		PRINT '=====================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=====================================';

		PRINT '-------------------------------------';
		PRINT 'Loading CRM Table';
		PRINT '-------------------------------------';

	-- Insert Data Into table using bulk insert
		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table :bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info; --Clear data 
		
		PRINT '>>>Inserting Data into :bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info -- Insert data into cus_info table
		FROM 'C:\Users\lappyfaris\OneDrive\Desktop\DATA ANALYSIS COURSE\SQL Full Course Data with Bara\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>>----------------------';

		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table :bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;
		
		PRINT '>>>Inserting Data into :bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info -- Insert data into prd_info table
		FROM 'C:\Users\lappyfaris\OneDrive\Desktop\DATA ANALYSIS COURSE\SQL Full Course Data with Bara\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>>----------------------';

		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table :bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details; -- Clear data
		PRINT '>>>Inserting Data into :bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details-- Insert data into table sales_details
		FROM 'C:\Users\lappyfaris\OneDrive\Desktop\DATA ANALYSIS COURSE\SQL Full Course Data with Bara\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>>----------------------';


		PRINT '-------------------------------------';
		PRINT 'Loading ERP Table';
		PRINT '-------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table :bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12; -- Clear data
		
		PRINT '>>>Inserting Data into :bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12 -- Insert data into table cust_az12
		FROM 'C:\Users\lappyfaris\OneDrive\Desktop\DATA ANALYSIS COURSE\SQL Full Course Data with Bara\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>>----------------------';

		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table :bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101; -- Clear data
		
		PRINT '>>>Inserting Data into :bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101 -- Insert Data into table loc_a101
		FROM 'C:\Users\lappyfaris\OneDrive\Desktop\DATA ANALYSIS COURSE\SQL Full Course Data with Bara\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>>----------------------';

		SET @start_time = GETDATE();
		PRINT '>>>Truncating Table :bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; -- Clear data
		
		PRINT '>>>Inserting Data into :bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2 -- Insert Data into table p_cat_g1v2
		FROM 'C:\Users\lappyfaris\OneDrive\Desktop\DATA ANALYSIS COURSE\SQL Full Course Data with Bara\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>Load Duration:' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>>----------------------';

		SET @end_batch = GETDATE();
		PRINT '>>>====================================';
		PRINT 'Bronze Layer Loads Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(second, @start_batch, @end_batch) AS NVARCHAR) + ' seconds';
		PRINT '>>>====================================';
		
	END TRY
	BEGIN CATCH
		PRINT '==============================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================='

	END CATCH
END;
