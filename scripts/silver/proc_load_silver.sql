/*
============================================================================================================
Stored Procedure: Load Silver layer (Bronze -> Silver)
============================================================================================================
Script Purpose:
  This stored procedure loads data into the 'silver' schema from the bronze schema.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Loads data from bronze tables to silver tables after undergoing data transformation

Parameters:
  None
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC silver.load_bronze;
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;;
	SET @batch_start = GETDATE();
	BEGIN TRY
		PRINT '=========================================================================';
		PRINT 'Loading Silver layer';
		PRINT '=========================================================================';
	
		PRINT '-------------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-------------------------------------------------------------------------';

		SET @start_time	= GETDATE();
	/*
	=====================================================================================================================
	CRM_CUST_INFO
	Data transformation on crm_cust_info table
	1) Data Cleansing: In cst_firstname and cst_lastname, removed any white space
	2) Data Normalization: In cst_marital status and cst_gndr, modified code/abbreviation into friendly values
	3) Data Enrichment: In cst_id, partitioned data by cst_id in order of cst_create date and chose only the newest data
	=====================================================================================================================
	*/
		PRINT '>> Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
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
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM (
			SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time	= GETDATE();
		/*
		====================================================================================================================
		CRM_CUST_INFO
		Data transformation on crm_cust_info table
		1) Derived Columns: In cat_id and prd_key, derived both columns out of prd_key column
		2) Missing Information: In prd_cost, used ISNULL to modify NULL values to 0
		3) Data Normalization: In prd_line, modified code/abbreviation into friendly values
		4) Data Type Casting: In prd_start_dt, modified data type from DATETIME to DATE
		5) Data Enrichment: In prd_end_dt, adding new, relevant data in addition to type casting from DATETIME to DATE
		====================================================================================================================
		*/
		PRINT '>> Truncating table: silver.crm_prod_info';
		TRUNCATE TABLE silver.crm_prod_info;
		PRINT '>> Inserting data into: silver.crm_prod_info';
		INSERT INTO silver.crm_prod_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, 
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prod_info

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time	= GETDATE();
		/*
		====================================================================================================================
		CRM_SALES_DETAILS
		Data transformation on crm_sales_details table
		1) Data Cleansing: In sls_ord_num, remove any white space using TRIM()
		2) Data Integrity: In sls_cust_id and sls_prd_key, check for any field that can't be used or aren't connected to 
						   prd_key and cst_id in crm_cust_info
		3) Data Type Casting: In sls_order_dt, sls_ship_dt, sls_due_dt, change INT value to DATE value
		4) Data Calculation: In sls_sales, sls_quantity, sls_price, ensure correct calculation (sales=quantity*price)
							 ** Change the silver.crm_sales_details DDL data type from INT to DATE to reflect the change **
		====================================================================================================================
		*/
		PRINT '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END sls_order_dt,
		CASE
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END sls_ship_dt,
		CASE
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END sls_due_dt,
		CASE
			WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price IS NULL OR sls_price <=0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END as sls_price
		FROM bronze.crm_sales_details
		/*
		1) WHERE sls_ord_num != TRIM(sls_ord_num)
		2) WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
		2) WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prod_info)

		3)   In sls_order_dt, sls_ship_dt, sls_due_dt, change integer value to date value
		3.1) Changed 0 value to NULL using NULLIF
		3.2) Checked dates <= 0 and length of date != 8
		3.3) Changed INT to DATE value using CASE WHEN
		3.4) Confirm order dates come earlier than ship dates

		3.2)
		SELECT 
		NULLIF(sls_order_dt, 0)
		FROM bronze.crm_sales_details
		WHERE sls_order_dt <= 0 or LEN(sls_order_dt) != 8

		SELECT 
		NULLIF(sls_ship_dt, 0)
		FROM bronze.crm_sales_details
		WHERE sls_ship_dt <= 0 or LEN(sls_ship_dt) != 8

		SELECT 
		NULLIF(sls_due_dt, 0)
		FROM bronze.crm_sales_details
		WHERE sls_due_dt <= 0 or LEN(sls_due_dt) != 8

		3.4)
		SELECT
		*
		FROM bronze.crm_sales_details
		WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_ship_dt

		4)   In sls_sales, sls_quantity, sls_price, ensure correct calculation (sales=quantity*price))
		4.1) Checked sales != quantity*price; and any of sales, quantity, or price are <=0 or IS NULL
		4.2) If sales is negative, zero, or null, derive it using quantity*price
		4.3) If price is zero or null, derive it using sales/quantity
		4.4) If price is negative, convert it to a positive value
		SELECT DISTINCT
		sls_sales AS old_sls_sales,
		sls_quantity,
		sls_price AS old_sls_price,
		-- 4.2)
		CASE
			WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END sls_sales,
		--4.3 & 4.4)
		CASE
			WHEN sls_price IS NULL OR sls_price <=0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END as sls_price
		FROM bronze.crm_sales_details
		-- 4.1)
		WHERE sls_sales != sls_quantity * sls_price
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
		OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
		ORDER BY sls_sales, sls_quantity, sls_price
		*/
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		PRINT '-------------------------------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '-------------------------------------------------------------------------';
		SET @start_time	= GETDATE();
		/*
		====================================================================================================================
		ERP_CUST_AZ12
		Data transformation on erp_cust_az12 table
		1) Invalid Value: Removed 'NAS%' and set future birthdates to null
		2) Data Normalization: In gen column, modified code to friendly values
		====================================================================================================================
		*/
		PRINT '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
			ELSE cid
		END cid,
		CASE
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END bdate,
		CASE
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') Then 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') Then 'Male'
			Else 'n/a'
		END gen
		FROM bronze.erp_cust_az12

		/*
		-- Identify out-of-range dates
		SELECT DISTINCT
		bdate
		FROM silver.erp_cust_az12
		WHERE bdate < '1925-01-01' OR bdate > GETDATE()

		-- Data standardization & consistency
		SELECT DISTINCT
		gen,
		CASE
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') Then 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') Then 'Male'
			Else 'n/a'
		END gen
		FROM silver.erp_cust_az12
		*/
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time	= GETDATE();
		/*
		====================================================================================================================
		ERP_LOC_A101
		Data transformation on erp_loc_a101 table
		1) Invalid Values: In cid, removed '-' to match with cst_key in silver.crm_cust_info
		2) Data Normalization: In cntry, standardized cntry values
		====================================================================================================================
		*/
		PRINT '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
		REPLACE(cid, '-', '') cid,
		CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ElSE TRIM(cntry)
		END cntry
		FROM bronze.erp_loc_a101
		ORDER BY cntry

		/*
		-- Data standardization % consistency
		SELECT DISTINCT cntry
		FROM silver.erp_loc_a101
		ORDER BY cntry
		*/
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @start_time	= GETDATE();
		/*
		====================================================================================================================
		ERP_PX_CAT_G1V2
		No data transformation on silver.erp_px_cat_g1v2 table
		====================================================================================================================
		*/
		PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)

		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2

		/*
		-- Check for unwanted spaces
		SELECT * FROM bronze.erp_px_cat_g1v2
		WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

		-- Data standardization & consistency
		SELECT DISTINCT
		cat
		FROM bronze.erp_px_cat_g1v2
		*/
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

		SET @batch_end = GETDATE();
		PRINT '=========================================================================';
		PRINT 'Successfully loaded Silver layer in ' + CAST(DATEDIFF(second, @batch_start, @batch_end) AS NVARCHAR) + ' seconds';
		PRINT '=========================================================================';
	END TRY
	BEGIN CATCH
		PRINT '========================================================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '========================================================================='
	END CATCH
END
