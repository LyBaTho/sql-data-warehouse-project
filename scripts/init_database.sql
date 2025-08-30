/*
Create Database and Data Schemas
	The script creates a new database named 'DataWarehouse' after checking if it exists.
	If the database exists, it will be dropped and recreated.
	The script sets up 3 schemas within the database: 'broze', 'silver', 'gold'

WARNING:
	Running this script will delete the entire 'DataWarehouse' database if it exists
*/

USE master;
GO

-- Drop & Recreate Data Warehouse
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP  DATABASE DataWarehouse;
END;
GO

-- Create Data Warehouse
CREATE DATABASE DataWarehouse;
GO
-- Switch to Data Warehouse
USE DataWarehouse;
GO

-- Create Schema
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
