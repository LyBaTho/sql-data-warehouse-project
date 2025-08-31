/*
===============================================================================
Quality Checks
===============================================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- silver.crm_cust_info
SELECT TOP(1000)
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM silver.crm_cust_info;

-- silver.crm_prd_info
SELECT TOP(1000) *
FROM silver.crm_prd_info;

-- silver.crm_sales_details
SELECT TOP(1000) *
FROM silver.crm_sales_details;

-- silver.erp_cust_az12
SELECT TOP(1000) *
FROM silver.erp_cust_az12;

-- silver.erp_loc_a101
SELECT TOP(1000) *
FROM silver.erp_loc_a101;

-- silver.erp_px_cat_g1v2
SELECT TOP(1000) *
FROM silver.erp_px_cat_g1v2;

-- Quality Check
--- Check for NULL or Duplicates in Primary Key
SELECT		cst_id, COUNT(*) AS cnt
FROM		bronze.crm_cust_info
GROUP BY	cst_id
HAVING		COUNT(*) > 1 OR cst_id IS NULL
ORDER BY	cnt DESC;

SELECT	* 
FROM	bronze.crm_cust_info
WHERE	cst_id IS NULL;

--- Check for Unwanted Spaces
SELECT		cst_firstname, cst_lastname
FROM		bronze.crm_cust_info
WHERE		cst_firstname != TRIM(cst_firstname) OR
			cst_lastname  != TRIM(cst_lastname);

-- Check for Unique Values
SELECT		DISTINCT cst_marital_status
FROM		bronze.crm_cust_info;

SELECT		DISTINCT cst_gndr
FROM		bronze.crm_cust_info;

-- Quality Check
--- Check for NULL or Duplicates in Primary Key (có duplicated; không có null, prd_end_dt có null)
SELECT		prd_key,
			COUNT(*) AS cnt
FROM		bronze.crm_prd_info
GROUP BY	prd_key
HAVING		COUNT(*) > 1
ORDER BY	cnt DESC;

SELECT		prd_key
FROM		bronze.crm_prd_info
WHERE		prd_key IS NULL;

SELECT		prd_start_dt
FROM		bronze.crm_prd_info
WHERE		prd_start_dt IS NULL;

SELECT		prd_end_dt
FROM		bronze.crm_prd_info
WHERE		prd_end_dt IS NULL;

--- Check for Unwanted Spaces
SELECT		prd_nm
FROM		bronze.crm_prd_info
WHERE		prd_nm != TRIM(prd_nm);

--- Check for Unique Values
SELECT		DISTINCT prd_cost
FROM		bronze.crm_prd_info;

SELECT		DISTINCT prd_line
FROM		bronze.crm_prd_info;	

--- Check for NUll or Negative values
SELECT		prd_cost
FROM		bronze.crm_prd_info
WHERE		prd_cost IS NULL OR prd_cost < 0;

--- Check for Start date is after End date
SELECT		*
FROM		bronze.crm_prd_info
WHERE		prd_end_dt < prd_start_dt;

-- Pick out one / two prd_id to explore
SELECT		prd_id,
			prd_key,
			prd_nm,
			prd_start_dt,
			prd_end_dt,
			LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM		bronze.crm_prd_info
WHERE		prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');

-- Quality Check
SELECT		*
FROM		bronze.crm_sales_details;

--- Check for NULL or Duplicates in Primary Key
SELECT		sls_ord_num,
			COUNT(*) AS cnt
FROM		bronze.crm_sales_details
GROUP BY	sls_ord_num
HAVING		COUNT(*) > 1;

--- Check for Unwanted spaces (sls_prd_key)
SELECT		sls_prd_key
FROM		bronze.crm_sales_details
WHERE		sls_ord_num != TRIM(sls_prd_key);

--- Check for invalid values
SELECT		sls_due_dt
FROM		bronze.crm_sales_details
WHERE		sls_due_dt <= 0 OR LEN(sls_due_dt) != 8;

SELECT		sls_due_dt
FROM		bronze.crm_sales_details
WHERE		LEN(sls_due_dt) != 8;

SELECT		NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM		bronze.crm_sales_details

SELECT		sls_sales, sls_quantity, sls_price
FROM		bronze.crm_sales_details
WHERE		sls_sales != sls_quantity * sls_price OR
			sls_sales IS NULL OR
			sls_quantity IS NULL OR
			sls_price IS NULL OR
			sls_sales <= 0 OR
			sls_quantity <= 0 OR
			sls_price <= 0;

SELECT		
			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_quantity) * ABS(sls_price) THEN ABS(sls_quantity) * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			CASE
				WHEN sls_price <= 0 OR sls_price IS NULL THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
FROM		bronze.crm_sales_details
WHERE		sls_sales < 0;
