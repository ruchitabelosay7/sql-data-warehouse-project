
--CHECKING FOR NULLS /DUPLICATES in bronze layer(primary key)
SELECT cst_id,COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id
having COUNT(*)>1 OR cst_id IS NULL

--checking extra spaces
select cst_firstname from bronze.crm_cust_info
where cst_firstname!=TRIM(cst_firstname)

--checking extra spaces
select cst_lastname from bronze.crm_cust_info
where cst_lastname!=TRIM(cst_lastname)

--checking extra spaces
select cst_gndr from bronze.crm_cust_info
where cst_gndr!=TRIM(cst_gndr)


--data standardization and consisitency
select 
distinct cst_marital_status
from bronze.crm_cust_info

select 
distinct cst_gndr
from bronze.crm_cust_info

-----------------------------------------------------------------------------------------------------------------

--CHECKING FOR NULLS /DUPLICATES in bronze layer(primary key)
SELECT cst_id,COUNT(*) FROM silver.crm_cust_info
GROUP BY cst_id
having COUNT(*)>1 OR cst_id IS NULL

--checking extra spaces
select cst_firstname from silver.crm_cust_info
where cst_firstname!=TRIM(cst_firstname)

--checking extra spaces
select cst_lastname from silver.crm_cust_info
where cst_lastname!=TRIM(cst_lastname)

--checking extra spaces
select cst_gndr from silver.crm_cust_info
where cst_gndr!=TRIM(cst_gndr)


--data standardization and consisitency
select 
distinct cst_marital_status
from silver.crm_cust_info

select 
distinct cst_gndr
from silver.crm_cust_info
============================================
select * from bronze.crm_prd_info

--checking for nulls/duplicates in primary key
select prd_id,count(*)
from silver.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null

select  prd_nm from silver.crm_prd_info
where prd_nm!=TRIM(prd_nm)

--check nulls ,or negative values
select prd_cost from silver.crm_prd_info
where prd_cost<0 or prd_cost is null

select * from silver.crm_prd_info
where prd_end_dt<prd_start_dt

SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt,
    DATEADD(day, -1,
        LEAD(prd_start_dt) OVER(
            PARTITION BY prd_key 
            ORDER BY prd_start_dt
        )
    ) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509','AC-HE-HL-U509-R');

-----------------------------------------------------------------------------------------
--checking for nulls and duplicates in ord_id
select sls_ord_num ,count(*) 
from silver.crm_sales_details
group by sls_ord_num
having count(*)>1 or sls_ord_num is null

select sls_ord_num
from bronze.crm_sales_details
where sls_ord_num!= TRIM(sls_ord_num)

--checking dates
select sls_due_dt
--NULLIF(sls_ship_dt,0)
from silver.crm_sales_details
where sls_due_dt<=0 or LEN(sls_due_dt)!=8
or sls_due_dt>20500101
or sls_due_dt<19000101


--checking for invalid date orders
select * from silver.crm_sales_details
where sls_ship_dt<sls_order_dt or sls_order_dt>sls_due_dt

--checking data consistencty between sales,quantity,price
--sales=quantity*price
--value must not be zero,null,or negative.
select * 
from silver.crm_sales_details
----------------------------------------------------------------------------
select * from bronze.erp_CUST_AZ12
select * from silver.crm_cust_info


SELECT 
    CASE 
        WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male' 
        ELSE 'n/a'
    END AS GEN,
	CASE 
		WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		ELSE CID
	END AS CID,

	CASE 
		WHEN BDATE>GETDATE() THEN NULL
		ELSE BDATE
	END BDATE
FROM bronze.erp_CUST_AZ12;





select BDATE from bronze.erp_CUST_AZ12
where BDATE>GETDATE()

select distinct gen from bronze.erp_CUST_AZ12

------------------------------------------------------------------------------------------
select 
CID,
REPLACE(CID,'-',''),
CASE
	WHEN TRIM(CNTRY)='DE' THEN 'Germany'
	WHEN TRIM(CNTRY) IN ('US','USA')THEN 'United States'
	WHEN TRIM(CNTRY)='' OR CNTRY IS NULL THEN 'N/A'
	ELSE CNTRY
END CNTRY
from bronze.erp_LOC_A101



select * from silver.crm_cust_info

select distinct CNTRY from bronze.erp_LOC_A101
-----------------------------------------------------
select * from bronze.erp_PX_CAT_G1V2
select * from silver.crm_prd_info
