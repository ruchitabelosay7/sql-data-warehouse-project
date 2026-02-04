--bulk insert(loading the data into tables)
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT'============================================';
		PRINT 'Loading Bronze Layer';
		PRINT'============================================';

		PRINT'----------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'----------------------------------------------';

		SET @start_time=GETDATE();
		PRINT '>> Truncating Table:bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>>Inserting Data into:bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Data Analyst\PROJECTS\SQL_PROJECT\D\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
		PRINT'==========================================';
		

		--SELECT COUNT(*) FROM bronze.crm_cust_info
		SET @start_time=GETDATE();
		PRINT '>> Truncating Table:bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data into:bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Data Analyst\PROJECTS\SQL_PROJECT\D\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
		PRINT'==========================================';

		--SELECT * FROM bronze.crm_prd_info
		SET @start_time=GETDATE();
		PRINT '>> Truncating Table:bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'>>Inserting Data into:bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Data Analyst\PROJECTS\SQL_PROJECT\D\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
		PRINT'==========================================';

		--select count(*) from bronze.crm_sales_details

		PRINT'----------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'----------------------------------------------';

		SET @start_time=GETDATE();
		PRINT '>> Truncating Table:bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT'>>Inserting data into:bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\Data Analyst\PROJECTS\SQL_PROJECT\D\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
		PRINT'==========================================';

		--select * from bronze.erp_LOC_A101
		SET @start_time=GETDATE();
		PRINT '>> Truncating Table:bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT'>>Inserting data into:bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\Data Analyst\PROJECTS\SQL_PROJECT\D\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
		PRINT'==========================================';

		--select * from bronze.erp_PX_CAT_G1V2;
		SET @start_time=GETDATE();
		PRINT '>> Truncating Table:bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT'>>Inserting data into:bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\Data Analyst\PROJECTS\SQL_PROJECT\D\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
		PRINT'==========================================';
		SET @batch_end_time=GETDATE();
		PRINT'================================================';
		PRINT'Total Load Duration: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds';
		PRINT'=====================================================';
	END TRY
	BEGIN CATCH
		PRINT'===============================================';
		PRINT'Error Occureed during loading bronze layer';
		PRINT'Error Message'+ERROR_MESSAGE();
		PRINT'Error Message'+CAST (ERROR_NUMBER()AS NVARCHAR);
		PRINT'Error Message'+CAST (ERROR_STATE()AS NVARCHAR);
		PRINT'===============================================';
	END CATCH
END
--select count(*) from bronze.erp_CUST_AZ12;
--stored procedure
EXEC bronze.load_bronze
