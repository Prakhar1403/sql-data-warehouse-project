--Creating Bronze layer table definitions for all tables
use Datawarehouse
if OBJECT_ID('bronze.crm_cust_info', 'U') is not null
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
		cst_id int,
		cst_key nvarchar(50),
		cst_firstname nvarchar(50),
		cst_lastname nvarchar(50),
		cst_material_status nvarchar(50),
		cst_gndr nvarchar(50),
		cst_create_date date
);
create table bronze.crm_prd_info(
		prd_id int,
		prd_key nvarchar(50),
		prd_nm nvarchar(50),
		prd_cost int,
		prd_line nvarchar(50),
		prd_start_dt date,
		prd_end_dt date
);
create table bronze.crm_sales_details(
		sls_ord_num nvarchar(50),
		sls_prd_key nvarchar(50),
		sls_cust_id int,
		sls_order_dt int,
		sls_ship_dt int,
		sls_due_dt int,
		sls_sales int,
		sls_quantity int,
		sls_price int
);
--So,Now we are done with the crm system ,now we need to define for ERP system

create table bronze.erp_loc_a101(
		cid nvarchar(50),
		cntry nvarchar(50)
);
create table bronze.erp_cust_az12(
		cid nvarchar(50),
		bdate date,
		gen nvarchar(50)
);
create table bronze.erp_px_cat_g1v2(
		id nvarchar(50),
		cat nvarchar(50),
		subcat nvarchar(50),
		maintenance nvarchar(50)
);
/* ==============================
     Loading Bulk Insert
=================================*/
CREATE OR ALTER PROCEDURE bronze.load_bronze as
begin
declare @whole_start_time datetime,@whole_end_time datetime
DECLARE @start_time DATETIME,@end_time DATETIME;
set @whole_start_time =getdate();
BEGIN TRY
	PRINT '==========================='
	PRINT'LOADING BRONZE LAYER'
	PRINT'============================'
	PRINT'--------------------------'
	PRINT'LOADING CRM TABLES'
	PRINT'--------------------------'

	SET @start_time=GETDATE();
	PRINT'>> TRUNCATING TABLE: [bronze].[crm_cust_info]'
	truncate table [bronze].[crm_cust_info]
	PRINT'>>INSERTING DATA INTO TABLE: [bronze].[crm_cust_info]'
	bulk insert [bronze].[crm_cust_info]
	from 'C:\Users\BizTecno\Desktop\dwhpj\DWH\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
		firstrow=2,--skip first row
		fieldterminator=',',
		tablock
	);
	
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';
	/*=======================================================
			Write this same script for all files and tables
	==========================================================*/
	
	
	SET @start_time=GETDATE();
	PRINT'>> TRUNCATING TABLE: [bronze].[crm_prd_info]'
	truncate table [bronze].[crm_prd_info]
	PRINT'>>INSERTING DATA INTO TABLE: [bronze].[crm_prd_info]'
	bulk insert [bronze].[crm_prd_info]
	from 'C:\Users\BizTecno\Desktop\dwhpj\DWH\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
		firstrow=2,--skip first row
		fieldterminator=',',
		tablock
	);
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';
	

	
	SET @start_time=GETDATE();
	PRINT'>> TRUNCATING TABLE: [bronze].[crm_sales_details]'
	truncate table [bronze].[crm_sales_details]
	PRINT'>>INSERTING DATA INTO TABLE: [bronze].[crm_sales_details]'
	bulk insert [bronze].[crm_sales_details]
	from 'C:\Users\BizTecno\Desktop\dwhpj\DWH\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
		firstrow=2,--skip first row
		fieldterminator=',',
		tablock
	);
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';

	PRINT'--------------------------'
	PRINT'LOADING ERP TABLES'
	PRINT'--------------------------'
	--For source system or ERP
	
	SET @start_time=GETDATE();
	PRINT'>> TRUNCATING TABLE: [bronze].[erp_cust_az12]'
	truncate table [bronze].[erp_cust_az12]
	PRINT'>>INSERTING DATA INTO TABLE: [bronze].[erp_cusrt_az12]'
	bulk insert [bronze].[erp_cust_az12]
	from 'C:\Users\BizTecno\Desktop\dwhpj\DWH\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with (
		firstrow=2,--skip first row
		fieldterminator=',',
		tablock
	);
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';

	
	SET @start_time=GETDATE();
	PRINT'>> TRUNCATING TABLE: [bronze].[erp_loc_a101]'
	truncate table [bronze].[erp_loc_a101]
	PRINT'>>INSERTING DATA INTO TABLE: [bronze].[erp_loc_a101]'
	bulk insert [bronze].[erp_loc_a101]
	from 'C:\Users\BizTecno\Desktop\dwhpj\DWH\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with (
		firstrow=2,--skip first row
		fieldterminator=',',
		tablock
	);
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';

	
	SET @start_time=GETDATE();
	PRINT'>>TRUNCATING TABLE:[bronze].[erp_px_cat_g1v2]'
	truncate table [bronze].[erp_px_cat_g1v2]
	PRINT'>>INSERTING DATA INTO:[bronze].[erp_px_cat_g1v2]'
	bulk insert [bronze].[erp_px_cat_g1v2]
	from 'C:\Users\BizTecno\Desktop\dwhpj\DWH\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with (
		firstrow=2,--skip first row
		fieldterminator=',',
		tablock
	);
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';

	END TRY
	BEGIN CATCH
	PRINT'-------------------------'
	PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT'ERROR MESSAGE:'+ERROR_MESSAGE()
	PRINT'ERROR NUMBER:'+CAST(ERROR_NUMBER() AS NVARCHAR)
	PRINT'ERROR STATE:'+CAST(ERROR_STATE() AS NVARCHAR)
	PRINT'-------------------------'
	END CATCH
	SET @whole_end_time=GETDATE();
	PRINT'>> TOTAL LOAD DURATION:'+cast(DATEDIFF(second,@whole_start_time,@whole_end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';

end
