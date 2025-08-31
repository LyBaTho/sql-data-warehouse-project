/*
  Produce quality check, cleanse, and insert the data to Silver Layer
  Note: To execute, please write EXEC silver.load_silver
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
		PRINT('====================================================');
		PRINT('Loading Silver Layer');
		PRINT('====================================================');

		PRINT('----------------------------------------------------');
		PRINT('Loading CRM Tables');
		PRINT('----------------------------------------------------');
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT('>> Loading Data Into silver.crm_cust_info');
		-- Cleansing and Inserting Value for table crm_cust_info
		INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname,cst_marital_status, cst_gndr, cst_create_date)
		SELECT
			cst_id, 
			cst_key, 
			TRIM(cst_firstname) AS cst_firstname, 
			TRIM(cst_lastname) AS cst_lastname,  
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM (
			SELECT		cst_id, 
						cst_key, 
						cst_firstname, 
						cst_lastname, 
						cst_marital_status, 
						cst_gndr, 
						cst_create_date,
						ROW_NUMBER()OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM		bronze.crm_cust_info) AS t
		WHERE		flag_last = 1 AND 
					cst_id IS NOT NULL;
		SET @end_time = GETDATE();
		PRINT('>> Loading Durations: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT('>> Loading Data Into silver.crm_prd_info');
		--- Cleansing and Inserting Value for table crm_prd_info
		INSERT INTO	silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
		SELECT		prd_id,
					REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
					SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
					TRIM(prd_nm) AS prd_nm,
					ISNULL(prd_cost,0) AS prd_cost,
					CASE 
						WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
						WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
						WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
						WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
						ELSE 'n/a'
					END prd_line,
					CAST(prd_start_dt AS DATE) AS prd_start_dt,
					CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
		FROM		bronze.crm_prd_info
		WHERE		prd_cost IS NOT NULL;
		SET @end_time = GETDATE();
		PRINT('>> Loading Durations: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT('>> Loading Data Into silver.crm_sales_details');
		--- Cleansing and Inserting Value for table crm_sales_details
		INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
		SELECT		sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					CASE
						WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
					END AS sls_order_dt,
					CASE
						WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
					END AS sls_ship_dt,
					CASE
						WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
					END AS sls_due_dt,
					CASE
						WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_quantity) * ABS(sls_price) THEN ABS(sls_quantity) * ABS(sls_price)
						ELSE sls_sales
					END AS sls_sales,
					sls_quantity,
					CASE
						WHEN sls_price <= 0 OR sls_price IS NULL THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
						ELSE sls_price
					END AS sls_price
		FROM		bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT('>> Loading Durations: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		PRINT('----------------------------------------------------');
		PRINT('Loading ERP Tables');
		PRINT('----------------------------------------------------');
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT('>> Loading Data Into silver.erp_cust_az12');
		-- Cleansing and Inserting Value for table erp_cust_az12
		INSERT INTO silver.erp_cust_az12(CID,BDATE,GEN)
		SELECT		
					CASE
						WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
						ELSE CID
					END AS CID,
					CASE
						WHEN BDATE > GETDATE() THEN NULL
						ELSE BDATE
					END AS BDATE,
					CASE
						WHEN TRIM(UPPER(GEN)) IN ('F','FEMALE') THEN 'Female'
						WHEN TRIM(UPPER(GEN)) IN ('M', 'MALE') THEN 'Male'
						ELSE 'n/a'
					END AS GEN
		FROM		bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT('>> Loading Durations: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');

		-- Cleansing and Inserting Value for table erp_loc_a101
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT('>> Loading Data Into silver.erp_loc_a101');

		INSERT INTO silver.erp_loc_a101(CID,CNTRY)
		SELECT		REPLACE(CID,'-','') AS CID,
					CASE
						WHEN TRIM(UPPER(CNTRY)) IN ('USA', 'UNITED STATES', 'US') THEN 'United States'
						WHEN TRIM(UPPER(CNTRY)) IN ('UNITED KINGDOM', '') THEN 'United Kingdom'
						WHEN TRIM(UPPER(CNTRY)) IN ('DE', 'GERMANY') THEN 'Germany'
						WHEN TRIM(UPPER(CNTRY)) IS NULL OR TRIM(UPPER(CNTRY)) = '' THEN 'n/a'
						ELSE TRIM(CNTRY)
					END AS CNTRY
		FROM		bronze.erp_loc_a101;

		-- Cleansing and Inserting Value for table erp_px_cat_g1v2
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT('>> Loading Data Into silver.erp_px_cat_g1v2')

		INSERT INTO	silver.erp_px_cat_g1v2(ID, CAT, SUBCAT, MAINTENANCE)
		SELECT		ID,
					CAT,
					SUBCAT,
					MAINTENANCE
		FROM		bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT('>> Loading Durations: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		SET @batch_end_time = GETDATE();
		PRINT('');
		PRINT('>> Total Loading Durations: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds');
END
