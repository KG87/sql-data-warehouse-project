/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER   PROCEDURE [bronze].[load_bronze] AS 
BEGIN
    DECLARE @start_time DATETIME, 
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME;
          --  @duration INT;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=============================================';
        PRINT '>> Starting Bronze Layer Data Load';
        PRINT '=============================================';


        PRINT '----------------------------------------------';
        PRINT '>> Loading CRM Tables';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        WITH  (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/data/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';



        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

        PRINT '----------------------------------------------';
        PRINT '>> Loading ERP Tables';
        PRINT '----------------------------------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/data/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
            );
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';


        SET @batch_end_time = GETDATE();
        PRINT '==============================================';
        PRINT '>> Bronze Layer Data Load Completed';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND , @batch_start_time, @batch_end_time) AS VARCHAR(10)) + ' seconds';
        PRINT '==============================================';
END TRY
BEGIN CATCH
PRINT '=============================================';
        PRINT '>> Error Occurred During Bronze Layer Data Load';
        PRINT '=============================================';

        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));

        -- Optionally, you can re-throw the error to propagate it
        -- RAISERROR(ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE());PR
END CATCH
    END

