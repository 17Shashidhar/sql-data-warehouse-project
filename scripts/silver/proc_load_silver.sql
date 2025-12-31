/*
=============================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================

Purpose:
    Performs ETL to populate Silver schema tables from Bronze.
    Applies data cleansing, standardization, and business rules.

Actions:
    - Truncates Silver tables
    - Inserts cleaned and transformed data from Bronze

Parameters:
    None

Usage:
    EXEC silver.load_silver;

Environment:
    Development / Learning
=============================================================
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batchstart_time DATETIME,
        @batchend_time DATETIME;

    BEGIN TRY
        SET @batchstart_time = GETDATE();

        PRINT '=============================================================';
        PRINT 'Loading Silver Layer';
        PRINT '=============================================================';

        ---------------------------------------------------------
        -- CRM CUSTOMER
        ---------------------------------------------------------
        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'N/A'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                ELSE 'N/A'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS rn
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE rn = 1;

        ---------------------------------------------------------
        -- CRM PRODUCT
        ---------------------------------------------------------
        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm,
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'N/A'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                DATEADD(DAY, -1,
                    LEAD(prd_start_dt)
                    OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
                ) AS DATE
            )
        FROM bronze.crm_prd_info;

        ---------------------------------------------------------
        -- CRM SALES
        ---------------------------------------------------------
        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            TRY_CONVERT(DATE, CAST(sls_order_dt AS VARCHAR(8))),
            TRY_CONVERT(DATE, CAST(sls_ship_dt AS VARCHAR(8))),
            TRY_CONVERT(DATE, CAST(sls_due_dt AS VARCHAR(8))),
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE ABS(sls_price)
            END
        FROM bronze.crm_sales_details;

        ---------------------------------------------------------
        -- ERP TABLES
        ---------------------------------------------------------
        TRUNCATE TABLE silver.erp_cust_az12;
        INSERT INTO silver.erp_cust_az12
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'N/A'
            END
        FROM bronze.erp_cust_az12;

        TRUNCATE TABLE silver.erp_loc_a101;
        INSERT INTO silver.erp_loc_a101
        SELECT
            REPLACE(cid, '-', ''),
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        INSERT INTO silver.erp_px_cat_g1v2
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @batchend_time = GETDATE();

        PRINT '=============================================================';
        PRINT 'Silver Layer Load Completed Successfully';
        PRINT 'Total Load Duration: '
              + CAST(DATEDIFF(SECOND, @batchstart_time, @batchend_time) AS NVARCHAR);
        PRINT '=============================================================';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURRED DURING SILVER LOAD';
        PRINT ERROR_MESSAGE();
    END CATCH
END;
GO
