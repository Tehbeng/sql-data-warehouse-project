/* 
==================================
Create New Database and Schemas
==================================

Script Purposes:
	This script is to create new database named 'DataWarehouse' before creating the database,
	do checking first to make sure it is not exist in the databases. If it is exist, it wil be 
	dropped and recreated. Lastly, the script sets up with three schemas which are bronze,
	silver and gold.

WARNING:
	Running this script will drop entire 'DataWarehouse' database if it is already exist.
	All data will be permenently delete. Proceed with caution and ensure that you have
	proper backups before running the script.
*/

USE master;
GO

--Drop and Recreate the 'DataWarehouse' database
IF EXIST (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;

GO

-- Create Database 'Data Warehouse
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;

GO

CREATE SCHEMA silver;

GO

CREATE SCHEMA gold;

GO
