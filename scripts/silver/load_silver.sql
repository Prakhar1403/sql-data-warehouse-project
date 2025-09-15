
create or alter procedure silver.load_silver as
begin
declare @whole_start_time datetime,@whole_end_time datetime
declare @start_time datetime,@end_time datetime
set @whole_start_time=GETDATE()
begin try
	PRINT '==========================='
	PRINT'LOADING SILVER LAYER'
	PRINT'============================'
	PRINT'--------------------------'
	PRINT'LOADING CRM TABLES'
	PRINT'--------------------------'

	set @start_time=getdate();
	print'TRUNCATING TABLE: silver.crm_cust_info'
	truncate table silver.crm_cust_info
	print'INSERTING INTO TABLE: silver.crm_cust_info'

	insert into silver.crm_cust_info(cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date
	)
	select cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when upper(trim(cst_material_status)) ='S' then 'Single'
		 when upper(trim(cst_material_status))='M' then 'Married'
		 else 'N/A'
	end cst_material_status,
	case when upper(trim(cst_gndr)) ='F' then 'Female'
		 when upper(trim(cst_gndr))='M' then 'Male'
		 else 'N/A'
	end cst_gndr,
	cst_create_date from (
	select *,row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null
	)as k where flag_last =1
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';


	set @start_time=getdate();
	print'TRUNCATING TABLE: silver.crm_prd_info'
	truncate table silver.crm_prd_info
	print'INSERTING INTO TABLE : silver.crm_prd_info'
	insert into silver.crm_prd_info (prd_id,
	prd_key,
	cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
	select prd_id,
	substring(prd_key,7,len(prd_key)) as prd_key,
	REPLACE(substring(prd_key,1,5),'-','_') as cat_id,
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
	case upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'N/A'
	END prd_line,
	cast(prd_start_dt as date) as prd_start_dt,
	cast(dateadd(day,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as date) as prd_end_dt
	from bronze.crm_prd_info
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';
	

	set @start_time=getdate();
	print'TRUNCATING TABLE: silver.crm_sales_details'
	truncate table silver.crm_sales_details
	print'INSERTING INTO TABLE : silver.crm_sales_details'
	INSERT into silver.crm_sales_details(
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
	select sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt =0 or len(sls_order_dt)!=8 then NULL
		 else cast(cast(sls_order_dt as varchar) as date)
	end sls_order_dt,
	case when sls_ship_dt =0 or len(sls_ship_dt)!=8 then NULL
		 else cast(cast(sls_ship_dt as varchar) as date)
	end sls_ship_dt,
	case when sls_due_dt =0 or len(sls_due_dt)!=8 then NULL
		 else cast(cast(sls_due_dt as varchar) as date)
	end sls_due_dt,
	case when sls_sales is null  or sls_sales <=0 or sls_sales!=sls_quantity*abs(sls_price) then sls_quantity*abs(sls_price) 
		 else sls_sales
	end sls_sales,
	sls_quantity,
	case when sls_price is null  or sls_price <=0  then abs(sls_sales)/nullif(sls_quantity,0) 
		 else sls_price
	end sls_price
	from bronze.crm_sales_details
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';


	PRINT'--------------------------'
	PRINT'LOADING ERP TABLES'
	PRINT'--------------------------'

	set @start_time=getdate();
	print'TRUNCATING TABLE: silver.erp_cust_az12'
	truncate table silver.erp_cust_az12
	print'INSERTING INTO TABLE : silver.erp_cust_az12'
	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	select 
	case when cid like'NAS%' then substring(cid,4,len(cid))
		 else cid
	end cid,
	case when bdate >getdate() then NULL
		 else bdate
	end bdate,
	case WHEN upper(trim(gen)) in ('M','MALE') then 'Male'
		 WHEN upper(trim(gen)) in ('F','FEMALE') then 'Female'
		 else 'N/A'
	END gen
	from bronze.erp_cust_az12
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';



	set @start_time=getdate();
	print'TRUNCATING TABLE: silver.erp_loc_a101'
	truncate table silver.erp_loc_a101
	print'INSERTING INTO TABLE : silver.erp_loc_a101'
	insert into silver.erp_loc_a101(
	cid
	,cntry
	)
	select 
	replace(cid,'-','') as cid,
	case when trim(cntry) in('USA','US') then 'United States'
	when trim(cntry) ='DE' then 'Germany'
	when trim(cntry)='' or trim(cntry) is null then 'N/A'
	else trim(cntry)
	end cntry 
	from bronze.erp_loc_a101
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';



	set @start_time=getdate();
	print'TRUNCATING TABLE: silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2
	print'INSERTING INTO TABLE : silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance)
	select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2
	SET @end_time=GETDATE();
	PRINT'>> LOAD DURATION:'+cast(DATEDIFF(second,@start_time,@end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';


	END TRY
	BEGIN CATCH
	PRINT'-------------------------'
	PRINT'ERROR OCCURED DURING LOADING SILVER LAYER'
	PRINT'ERROR MESSAGE:'+ERROR_MESSAGE()
	PRINT'ERROR NUMBER:'+CAST(ERROR_NUMBER() AS NVARCHAR)
	PRINT'ERROR STATE:'+CAST(ERROR_STATE() AS NVARCHAR)
	PRINT'-------------------------'
	END CATCH
	SET @whole_end_time=GETDATE();
	PRINT'>> TOTAL LOAD DURATION:'+cast(DATEDIFF(second,@whole_start_time,@whole_end_time) as nvarchar)+' seconds';
	print'------------------------------------------------';


end

exec silver.load_silver
