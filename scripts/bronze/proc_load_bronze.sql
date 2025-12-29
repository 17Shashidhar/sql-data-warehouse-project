/*
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema
    from external CSV files.

    Actions performed:
    - Truncates bronze tables before loading data
    - Uses BULK INSERT to load CSV data
    - Logs load duration and execution status

Parameters:
    None

Usage Example:
    EXEC bronze.load_bronze;

Environment:
    Development / Learning
=============================================================
*/

USE DataWarehouse;
GO

-------------------------------------------------------------
-- Create or Alter Stored Procedure
-------------------------------------------------------------
CREATE OR ALTER PROCEDURE bronze.load_bronze
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
        PRINT 'Loading Bronze Layer';
        PRINT '=============================================================';

        ---------------------------------------------------------
        -- Load CRM Tables
        ---------------------------------------------------------
        PRINT 'Loading CRM Tables';

        -- CRM Customer Info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\DELL\OneDrive\Desktop\warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'crm_cust_info load time: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- CRM Product Info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\DELL\OneDrive\Desktop\warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'crm_prd_info load time: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- CRM Sales Details
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\DELL\OneDrive\Desktop\warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'crm_sales_details load time: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        ---------------------------------------------------------
        -- Load ERP Tables
        ---------------------------------------------------------
        PRINT 'Loading ERP Tables';

        -- ERP Customer
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\DELL\OneDrive\Desktop\warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'erp_cust_az12 load time: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ERP Location
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\DELL\OneDrive\Desktop\warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'erp_loc_a101 load time: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ERP Product Category
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\DELL\OneDrive\Desktop\warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'erp_px_cat_g1v2 load time: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batchend_time = GETDATE();

        PRINT '=============================================================';
        PRINT 'Bronze Layer Load Completed Successfully';
        PRINT 'Total Load Duration: '
              + CAST(DATEDIFF(SECOND, @batchstart_time, @batchend_time) AS NVARCHAR) + ' seconds';
        PRINT '=============================================================';
    END TRY
    BEGIN CATCH
        PRINT '=============================================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=============================================================';
    END CATCH
END;
GO
