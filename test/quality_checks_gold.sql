select * from gold.dim_customers
select * from gold.dim_products

select * from gold.fact_sales

SELECT prd_key,COUNT(*) FROM(		
SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.CAT,
pc.SUBCAT,
pc.MAINTENANCE
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_PX_CAT_G1V2 pc
ON pn.cat_id=pc.ID
WHERE pn.prd_end_dt IS NULL --only current data
)t GROUP BY prd_key
HAVING COUNT(*)>1


SELECT
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS  price
from silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id
