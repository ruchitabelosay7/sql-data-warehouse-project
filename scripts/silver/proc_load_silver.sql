--loads silver lyer (bonze->silver)
--Usage:exec SILVER.load_silver

---------------------------------------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time=GETDATE();
	PRINT'======================================================';
	PRINT 'Loading Silver Layer';
	PRINT'==============================================================';

	PRINT'======================================================';
	PRINT 'Loading CRM tables';
	PRINT'==============================================================';

	--loading silver.crm_cust_info
	SET @start_time=GETDATE();
		print('>>truncating the table: silver.crm_cust_info');
		TRUNCATE TABLE silver.crm_cust_info
		print('inserting data into :silver.crm_cust_info');
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)

		select
		cst_id,
		cst_key,
		TRIM(cst_firstname)AS cst_firstname,
		TRIM(cst_lastname)AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status))='S' then 'Single'
			WHEN UPPER(TRIM(cst_marital_status))='M' then 'Married'
			ELSE 'N/A' END cst_marital_status,

		CASE WHEN UPPER(TRIM(cst_gndr))='F' then 'Female'
			WHEN UPPER(TRIM(cst_gndr))='M' then 'Male'
			ELSE 'N/A' END cst_gnr,
		cst_create_date
		from(
		select * ,
		ROW_NUMBER() OVER(partition by cst_id order by cst_create_date desc)rn
		from bronze.crm_cust_info 
		where cst_id IS NOT NULL
		)t
		where rn=1
		SET @end_time=GETDATE();
		PRINT'>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT'>>----------';
		
		SET @start_time=GETDATE();
		--inserting into silver layer(crm_prd_info)
		print('>>truncating the table: silver.crm_prd_info');
		TRUNCATE TABLE silver.crm_prd_info
		print('>>inserting data into :silver.crm_prd_info');
		insert into silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)


		select
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_')AS cat_id,--extracts category id
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,--extracts product key
		prd_nm,
		ISNULL(prd_cost,0)AS prd_cost,
		CASE
			WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
			WHEN UPPER(TRIM(prd_line))='S' THEN 'Other sales'
			ELSE 'N/A' END prd_line,
		CAST(prd_start_dt AS DATE)AS prd_start_dt,
		 CAST(
			DATEADD(
				day, 
				-1,
				LEAD(prd_start_dt) OVER (
					PARTITION BY prd_key
					ORDER BY prd_start_dt
				)
			) 
		AS DATE) AS prd_end_dt --calculates end_date as one date before the next start date
		from bronze.crm_prd_info
		SET @end_time=GETDATE();
		PRINT'>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT'>>----------';

		SET @start_time=GETDATE();
		print'>> Truncating Table :silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details
		print'>>--Inserting Data into: silver.crm_sales_details '
		insert into silver.crm_sales_details(
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

		select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt<=0 OR LEN(sls_order_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS varchar)AS DATE) 
			END sls_order_dt,
		CASE WHEN sls_ship_dt<=0 OR LEN(sls_ship_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS varchar)AS DATE) 
			END sls_ship_dt,
		CASE WHEN sls_due_dt<=0 OR LEN(sls_due_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS varchar)AS DATE) 
			END sls_due_dt,
		case when sls_sales<=0 OR sls_sales IS NULL OR sls_sales!=sls_quantity*ABS(sls_price)
			THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
			END AS sls_sales,
		sls_quantity,
		case when sls_price<=0 OR sls_price IS NULL
			THEN sls_sales/NULLIF(ABS(sls_quantity),0)
			ELSE sls_price
			END AS sls_price
		from bronze.crm_sales_details
		SET @end_time=GETDATE();
		PRINT'>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT'>>----------';

		SET @start_time=GETDATE();
		print'>>Truncating Table :silver.erp_CUST_AZ12 '
		TRUNCATE TABLE silver.erp_CUST_AZ12
		print'>>--Inserting Data into: silver.erp_CUST_AZ12 ';

		INSERT INTO silver.erp_CUST_AZ12(CID,BDATE,GEN)

		SELECT 
  
			CASE 
				WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
				ELSE CID
			END AS CID,

			CASE 
				WHEN BDATE>GETDATE() THEN NULL
				ELSE BDATE
			END BDATE,
			  CASE 
				WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male' 
				ELSE 'n/a'
			END AS GEN
		FROM bronze.erp_CUST_AZ12;
		SET @end_time=GETDATE();
		PRINT'>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT'>>----------';

		SET @start_time=GETDATE();
		print'>>Truncating Table :silver.erp_LOC_A101 '
		TRUNCATE TABLE silver.erp_LOC_A101
		print'>>--Inserting Data into: silver.erp_LOC_A101 ';
		insert into silver.erp_LOC_A101(CID,CNTRY)

		select 
		REPLACE(CID,'-','') AS CID,
		CASE
			WHEN TRIM(CNTRY)='DE' THEN 'Germany'
			WHEN TRIM(CNTRY) IN ('US','USA')THEN 'United States'
			WHEN TRIM(CNTRY)='' OR CNTRY IS NULL THEN 'N/A'
			ELSE CNTRY
		END CNTRY
		from bronze.erp_LOC_A101
		SET @end_time=GETDATE();
		PRINT'>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT'>>----------';

		SET @start_time=GETDATE();
		print'>> Truncating Table :silver.erp_PX_CAT_G1V2';
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2
		print'>>--Inserting Data into: silver.erp_PX_CAT_G1V2 ';
		insert into silver.erp_PX_CAT_G1V2( 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		)

		select 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		from bronze.erp_PX_CAT_G1V2
		SET @end_time=GETDATE();
		PRINT'>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
		PRINT'>>----------';

		SET @batch_end_time=GETDATE();
			PRINT'======================================================';
			PRINT 'Loading Silver Layer is Completed';
			PRINT'==============================================================';
			PRINT'>>Total Load duration: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR)+ ' seconds';
			PRINT'>>----------';

	END TRY
	BEGIN CATCH
		PRINT'=========================================';
		PRINT'Error occured during loading';
		PRINT'Error Message'+ERROR_MESSAGE();
		PRINT'Error Message'+CAST (ERROR_NUMBER()AS NVARCHAR);
		PRINT'Error Message'+CAST (ERROR_STATE()AS NVARCHAR);
		PRINT'===============================================';
	END CATCH
END
