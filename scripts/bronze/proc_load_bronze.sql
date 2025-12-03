CREATE OR ALTER PROCEDURE bronze.load_bronze AS
DECLARE @start_time_batch DATETIME, @end_time_batch DATETIME;
SET @start_time_batch = GETDATE();
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY

		PRINT '==============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting data: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\DW-Project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '>> ------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting data: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\DW-Project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '>> ------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\DW-Project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '>> ------------------';


		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting data: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\DW-Project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '>> ------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\DW-Project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '>> ------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\DW-Project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME :' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		PRINT '>> ------------------';

		SET @end_time_batch = GETDATE();
		PRINT '>> BRONZE LAYER LOADING TIME :' + CAST (DATEDIFF(second, @start_time_batch, @end_time_batch) AS NVARCHAR) + ' SECONDS';
		PRINT '>> ------------------';
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';
	END CATCH
END
