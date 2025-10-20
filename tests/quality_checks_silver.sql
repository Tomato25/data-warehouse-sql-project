/*
===========================================================================
Quality Checks
===========================================================================
Script Purpose:
This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schemas. It includes checks for:
- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardization and consistency.
- Invalid date ranges and orders.
- Data consistency between related fields.

Usage Notes:
- Run these checks after data loading Silver Layer.
- Investigate and resolve any discrepancies found during the checks.

===========================================================================
*/



-- Check for nulls and duplicates in Primary Key
-- Excpectation: No result

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Check for unwanted spaces
-- Expectation: No results

SELECT cst_lastname 
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info


-- Check for nulls and duplicates in Primary Key
-- Excpectation: No result

SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for NULLs values or negative numbers
SELECT prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

-- Data Standardization & Consistency

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_Start_dt




-- Check for Invalid Date 
SELECT 
NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101
OR sls_due_dt < 19500101

-- Check for invalid Date orders 
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check for invalid price,quantity and sales

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity 
OR sls_sales <= 0 OR sls_sales IS NULL OR sls_price IS NULL OR sls_quantity IS NULL 
OR sls_price <= 0 OR sls_quantity <= 0 

SELECT * FROM silver.crm_sales_details


-- Identify Out-of-range dates

SELECT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency 

SELECT DISTINCT 
gen
FROM silver.erp_cust_az12 


-- Data Standardization & Consistency 

SELECT DISTINCT 
cntry
FROM silver.erp_loc_a101
ORDER BY cntry



 -- CHEck for unwanted spaces 
 SELECT * FROM bronze.erp_px_cat_g1v2
 WHERE subcat != TRIM(subcat)

 -- Data Standardization & Consistency 
SELECT DISTINCT 
maintenance
FROM bronze.erp_px_cat_g1v2
ORDER BY maintenance
