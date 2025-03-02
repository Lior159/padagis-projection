--CREATE OR ALTER VIEW [presentation].[v_projection_fact_inventory] AS
WITH 
otif AS (
	SELECT  
		material AS material_id,
		MAX(planned_delivery_time) AS planned_delivery_time
	FROM [presentation].[mng_fact_otif_sap]
	GROUP BY material
)
,first_day_of_current_period AS (
	SELECT 
		date
	FROM dwh.general_dim_date_fiscal 
	WHERE first_day_of_period = 1
	  AND FiscalPeriod = (SELECT FiscalPeriod FROM dwh.general_dim_date_fiscal WHERE date = CAST(GETDATE() AS DATE))
	  AND FiscalYear = (SELECT FiscalYear FROM dwh.general_dim_date_fiscal WHERE date = CAST(GETDATE() AS DATE))
)
,inventory AS (
	SELECT 
		material_no AS material_id
		,inv.date
		,unrestricted_inventory AS current_inventory
		,ROW_NUMBER() OVER (PARTITION BY material_no order by inv.date DESC) rnk 
	FROM [PowerBI].[presentation].[moe_fact_inv] AS inv
	CROSS JOIN first_day_of_current_period
	WHERE [material_no] IS NOT NULL 
		AND YEAR(inv.[date]) = 2025
		AND inv.date <= first_day_of_current_period.date
)
,plan_last_doc_date AS (
	SELECT 
		site,
		MAX(doc_date) AS last_date
	FROM dwh.projection_production_plan
	GROUP BY site
)
,production_plan AS (
	SELECT 
		ISNULL(production_plan.material_id, material_mapping.material_id) AS material_id
		,production_plan.pil_material_id
		,CAST(otif_dim_date.FiscalPeriod AS INT) AS fiscal_period
		,CAST(otif_dim_date.FiscalYear  AS INT) AS fiscal_year
		,dim_date.date AS first_day_of_period
		,production_plan.units_plan
		,production_plan.site
	FROM dwh.projection_production_plan AS production_plan
	JOIN plan_last_doc_date 
		ON production_plan.site = plan_last_doc_date.site
		AND production_plan.doc_date = plan_last_doc_date.last_date
	LEFT JOIN [dwh].[projection_material_id_mapping] AS material_mapping
		ON production_plan.pil_material_id = material_mapping.pil_material_id
		AND production_plan.material_id IS NULL
	LEFT JOIN otif 
		ON ISNULL(production_plan.material_id, material_mapping.material_id) = otif.material_id
	JOIN dwh.general_dim_date_fiscal AS otif_dim_date
		ON DATEADD(
			DAY, 
			ISNULL(otif.planned_delivery_time, 0), 
			CONCAT(production_plan.fiscal_year, '-', production_plan.fiscal_period, '-', '15')) = otif_dim_date.date
	JOIN dwh.general_dim_date_fiscal AS dim_date
		ON otif_dim_date.FiscalYear = dim_date.FiscalYear
		AND otif_dim_date.FiscalPeriod = dim_date.FiscalPeriod
		AND dim_date.first_day_of_period = 1
	WHERE ISNULL(production_plan.material_id, material_mapping.material_id) IS NOT NULL
)
,pil_coid AS (
	SELECT
		material_mapping.material_id,
		dim_date.FiscalPeriod AS fiscal_period,
		dim_date.FiscalYear AS fiscal_year,
		SUM(coid.target_qty) AS units_forecast
	FROM [dwh].[projection_pil_coid] AS coid
	LEFT JOIN [dwh].[projection_material_id_mapping] AS material_mapping
		ON coid.material_id = material_mapping.pil_material_id
	LEFT JOIN otif
		ON material_mapping.material_id = CAST(otif.material_id AS VARCHAR)
	JOIN [presentation].[general_dim_date_fiscal] AS dim_date
		ON DATEADD(DAY, ISNULL(otif.planned_delivery_time, 0), coid.bsc_start) = dim_date.date
	JOIN first_day_of_current_period 
		ON coid.doc_date = first_day_of_current_period.date
	--WHERE coid.doc_name = 'COID-PIL_20250101_040003.csv'
	GROUP BY material_mapping.material_id, dim_date.FiscalPeriod, dim_date.FiscalYear
)
,pmn_coid AS (
	SELECT
		coid.material_id,
		dim_date.FiscalPeriod AS fiscal_period,
		dim_date.FiscalYear AS fiscal_year,
		SUM(coid.target_qty) AS units_forecast
	FROM [dwh].[projection_pmn_coid] AS coid
	LEFT JOIN otif
		ON coid.material_id = CAST(otif.material_id AS VARCHAR)
	JOIN [presentation].[general_dim_date_fiscal] AS dim_date
		ON DATEADD(DAY, ISNULL(otif.planned_delivery_time, 0), coid.bsc_start) = dim_date.date
	--JOIN first_day_of_current_period 
	--	ON coid.doc_date = first_day_of_current_period.date
	GROUP BY coid.material_id, dim_date.FiscalPeriod, dim_date.FiscalYear
)
,production_forecast AS (
	SELECT
		material_id,
		fiscal_period,
		fiscal_year,
		units_forecast,
		'PIL' AS site
	FROM pil_coid
	UNION ALL 
	SELECT
		material_id,
		fiscal_period,
		fiscal_year,
		units_forecast,
		'PMN' AS site
	FROM pmn_coid
)
,spi AS (
	SELECT 
		ISNULL(production_plan.material_id, sales.material_id) AS material_id
		,ISNULL(production_plan.fiscal_year, sales.fiscal_year) AS fiscal_year
		,ISNULL(production_plan.fiscal_period, sales.fiscal_period) AS fiscal_period
		,ISNULL(production_plan.first_day_of_period, sales.first_day_of_period) AS first_day_of_period
		,ISNULL(sales.units_plan,0) AS sales_plan
		,ISNULL(sales.units_forecast, 0) AS sales_forecast
		,ISNULL(production_plan.units_plan, 0) AS production_plan
		,ISNULL(production_forecast.units_forecast, 0) AS production_forecast
		,ISNULL(inventory.current_inventory, 0) AS current_inventory
		,production_plan.site
	FROM production_plan
	LEFT JOIN production_forecast 
		ON production_plan.material_id = production_forecast.material_id
		AND production_plan.fiscal_year = production_forecast.fiscal_year
		AND production_plan.fiscal_period = production_forecast.fiscal_period
		AND production_plan.site = production_forecast.site
	FULL JOIN [presentation].[v_projection_fact_sales] AS sales
		ON production_plan.material_id = sales.material_id
		AND production_plan.fiscal_year = sales.fiscal_year
		AND production_plan.fiscal_period = sales.fiscal_period
	LEFT JOIN inventory
		ON ISNULL(production_plan.material_id, sales.material_id) = inventory.material_id
		AND rnk = 1
	CROSS JOIN first_day_of_current_period
	WHERE ISNULL(production_plan.first_day_of_period, sales.first_day_of_period) >= first_day_of_current_period.date
)
,inventory_projection AS (
	SELECT 
		material_id
		,fiscal_year
		,fiscal_period
		,first_day_of_period
		,sales_plan
		,SUM(sales_plan) OVER (PARTITION BY material_id, fiscal_year ORDER BY fiscal_period) AS sales_plan_running_sum
		,sales_forecast
		,SUM(sales_forecast) OVER (PARTITION BY material_id, fiscal_year ORDER BY fiscal_period) AS sales_forecast_running_sum
		,production_plan
		,SUM(production_plan) OVER (PARTITION BY material_id, fiscal_year ORDER BY fiscal_period) AS production_plan_running_sum
		,production_forecast
		,SUM(production_forecast) OVER (PARTITION BY material_id, fiscal_year ORDER BY fiscal_period) AS production_forecast_running_sum
		,current_inventory
		,site
	FROM spi
)
SELECT 
	material_id
	,fiscal_year
	,fiscal_period
	,first_day_of_period
	,sales_plan
	,sales_plan_running_sum
	,sales_forecast
	,sales_forecast_running_sum
	,production_plan
	,production_plan_running_sum
	,production_forecast
	,production_forecast_running_sum
	,current_inventory
	,current_inventory + production_plan_running_sum - sales_plan_running_sum AS inventoy_plan
	,current_inventory + production_forecast_running_sum - sales_forecast_running_sum AS inventoy_forecast
	,site
FROM inventory_projection 
--CROSS JOIN first_day_of_current_period
--WHERE first_day_of_period >= date