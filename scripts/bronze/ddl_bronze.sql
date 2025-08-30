/*
	Create empty tables to contain data
	Bulk insert all the data into created tables
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info

CREATE TABLE bronze.crm_cust_info(
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status	NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info

CREATE TABLE bronze.crm_prd_info(
	prd_id				INT,
	prd_key				NVARCHAR(50),
	prd_nm				NVARCHAR(50),
	prd_cost			INT,
	prd_line			NVARCHAR(50),
	prd_start_dt		DATETIME,
	prd_end_dt			DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details

CREATE TABLE bronze.crm_sales_details(
	sls_ord_num			NVARCHAR(50),
	sls_prd_key			NVARCHAR(50),
	sls_cust_id			INT,
	sls_order_dt		INT,
	sls_ship_dt			INT,
	sls_due_dt			INT,
	sls_sales			INT,
	sls_quantity		INT,
	sls_price			INT
);
GO

IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12

CREATE TABLE bronze.erp_cust_az12(
	CID					NVARCHAR(50),
	BDATE				DATE,
	GEN					NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101

CREATE TABLE bronze.erp_loc_a101(
	CID					NVARCHAR(50),
	CNTRY				NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2

CREATE TABLE bronze.erp_px_cat_g1v2(
	ID					NVARCHAR(50),
	CAT					NVARCHAR(50),
	SUBCAT				NVARCHAR(50),
	MAINTENANCE			NVARCHAR(50)
);
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
		PRINT('====================================================');
		PRINT('Loading Bronze Layer');
		PRINT('====================================================');

		PRINT('----------------------------------------------------');
		PRINT('Loading CRM Tables');
		PRINT('----------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('Truncating: Table bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT('Inserting: Table bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\4. Jobs\1. Resume\1. PORTFOLIO\Single Project\3. Data Warehouse Construction\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		SET @start_time = GETDATE();
		PRINT('Truncating: Table bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT('Inserting: Table bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\4. Jobs\1. Resume\1. PORTFOLIO\Single Project\3. Data Warehouse Construction\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		SET @start_time = GETDATE();
		PRINT('Truncating: Table bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT('Inserting: Table bronze.crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\4. Jobs\1. Resume\1. PORTFOLIO\Single Project\3. Data Warehouse Construction\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT ('>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		PRINT('----------------------------------------------------');
		PRINT('Loading ERP Tables');
		PRINT('----------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('Truncating: Table bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT('Inserting: Table bronze.erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\4. Jobs\1. Resume\1. PORTFOLIO\Single Project\3. Data Warehouse Construction\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		SET @start_time = GETDATE();
		PRINT('Truncating: Table bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT('Inserting: Table bronze.erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\4. Jobs\1. Resume\1. PORTFOLIO\Single Project\3. Data Warehouse Construction\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		SET @start_time = GETDATE();
		PRINT('Truncating: Table bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT('Inserting: Table bronze.erp_px_cat_g1v2');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\4. Jobs\1. Resume\1. PORTFOLIO\Single Project\3. Data Warehouse Construction\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Loading Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		SET @batch_end_time = GETDATE();
		PRINT('')
		PRINT('Loading Bronze Layer is Completed')
		PRINT('>> Total Loading Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds');
END

EXEC bronze.load_bronze;
