
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/



SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE OR ALTER   PROCEDURE [silver].[load_silver] AS 
BEGIN
    DECLARE @start_time DATETIME, 
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME;
          --  @duration INT;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=============================================';
        PRINT '>> Starting Silver Layer Data Load';
        PRINT '=============================================';


        PRINT '----------------------------------------------';
        PRINT '>> Loading CRM Tables';
        PRINT '----------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        
            INSERT INTO DataWarehouse.silver.crm_cust_info (
                cst_id,
                cst_key,
                cst_first_name,
                cst_last_name,
                cst_material_status,
                cst_gender,
                cst_create_date)
            SELECT 
            cst_id,
            cst_key,
            TRIM(cst_first_name) AS cst_first_name,
            TRIM(cst_last_name) AS cst_last_name,
            CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'n/a' 
                END AS cst_material_status,
            CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
                ELSE 'n/a' 
                END cst_gender,
                cst_create_date
            FROM (

            SELECT
            *,
            RANK() OVER (PARTITION BY  cst_id ORDER BY cst_create_date DESC) AS rnk
            FROM
            DataWarehouse.bronze.crm_cust_info
            --WHERE cst_id = 29466
            ) t WHERE rnk = 1 

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';
        PRINT '----------------------------------------------';



        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        
        INSERT INTO [DataWarehouse].[silver].[crm_prd_info]
        ([prd_id]
        ,[cat_id]
        ,[prd_key]
        ,[prd_nm]
        ,[prd_cost]
        ,[prd_line]
        ,[prd_start_dt]
        ,[prd_end_dt])

        SELECT 
            [prd_id]
            ,REPLACE(SUBSTRING([prd_key], 1, 5), '-', '_') as cat_id
            ,SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key
            ,[prd_nm]
            ,ISNULL(prd_cost, 0) as prd_cost
            ,CASE UPPER(TRIM(prd_line))
            WHEN  'M' THEN 'Mountain'
                    WHEN  'R' THEN 'Road'
                    WHEN  'S' THEN 'Other Sales'
                    WHEN  'T' THEN 'Touring'
                    ELSE 'n/a' 
            END AS prd_line
            ,CAST(prd_start_dt AS DATE) AS prd_start_dt
            ,LEAD(prd_start_dt) OVER (PARTITiON BY prd_key ORDER BY prd_start_dt)-1 as prd_end_dt
        FROM [DataWarehouse].[bronze].[crm_prd_info]
        WHERE prd_key IS NOT NULL;       


        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';



        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        

        INSERT INTO [DataWarehouse].[silver].[crm_sales_details]
                    ([sls_ord_num]
                    ,[sls_prd_key]
                    ,[sls_cust_id]
                    ,[sls_order_dt]
                    ,[sls_ship_dt]
                    ,[sls_due_dt]
                    ,[sls_sales]
                    ,[sls_quantity]
                    ,[sls_price])
        SELECT  
             [sls_ord_num]
            ,[sls_prd_key]
            ,[sls_cust_id]
            ,CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
            END AS sls_order_dt
            ,CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
            END AS sls_ship_dt
            ,CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
            END AS sls_due_dt
            ,CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales 
                END AS sls_sales
            ,[sls_quantity]
            ,CASE WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0) 
                ELSE sls_price 
                END AS sls_price
        FROM [DataWarehouse].[bronze].[crm_sales_details]
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';

        PRINT '----------------------------------------------';
        PRINT '>> Loading ERP Tables';
        PRINT '----------------------------------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO [DataWarehouse].[silver].[erp_loc_a101] (
        [cid]
        ,[cntry]
        )

        SELECT 
            REPLACE(cid, '-', '') cid_clean
            ,CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                    WHEN TRIM(cntry) LIKE ('US%') THEN 'United States'
                    WHEN TRIM(cntry) LIKE ('USA%') THEN 'United States'

                    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' 
                    ELSE TRIM(cntry)
            END AS cntry
        FROM [DataWarehouse].[bronze].[erp_loc_a101]
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';

        INSERT INTO [DataWarehouse].[silver].[erp_cust_az12] (
        cid
        ,bdate
        ,gen
        -- , dwh_create_date
        )
        SELECT  
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                    ELSE cid 
                    END AS cid
            ,CASE WHEN bdate > GETDATE() THEN NULL
                    ELSE bdate 
                    END AS bdate
                    ,CASE 
            WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
            WHEN UPPER(TRIM(gen)) LIKE 'M%' THEN 'Male'
            ELSE 'n/a'
        END AS [gen]
            -- ,[dwh_create_date]
        FROM [DataWarehouse].[bronze].[erp_cust_az12]

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT [id]
            ,[cat]
            ,[subcat]
            ,CASE 
                WHEN UPPER(TRIM(maintenance)) LIKE 'YES%' THEN 'Yes'
                WHEN UPPER(TRIM(maintenance)) LIKE 'NO%' THEN 'No'
                ELSE 'n/a'
                END AS maintenance
        FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR(10)) + ' seconds';


        SET @batch_end_time = GETDATE();
        PRINT '==============================================';
        PRINT '>> Silver Layer Data Load Completed';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND , @batch_start_time, @batch_end_time) AS VARCHAR(10)) + ' seconds';
        PRINT '==============================================';
END TRY
BEGIN CATCH
PRINT '=============================================';
        PRINT '>> Error Occurred During Silver Layer Data Load';
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

