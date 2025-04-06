CREATE OR ALTER VIEW [presentation].[v_projection_fact_inventory] AS
WITH 
dim_date AS (
	SELECT 
		date,
		CAST(FiscalYear AS INT) AS fiscal_year,
		CAST(FiscalPeriod AS INT) AS fiscal_period,
		MIN(DATE) OVER(PARTITION BY FiscalYear, FiscalPeriod) AS first_day_of_period,
		IIF(MIN(DATE) OVER(PARTITION BY FiscalYear, FiscalPeriod) = DATE, 1,0) is_first_day_of_period
	FROM dwh.general_dim_date_fiscal
),
otif AS (
	SELECT DISTINCT
		material AS material_id,
		LAST_VALUE(planned_delivery_time) OVER (
			PARTITION BY material 
			ORDER BY scheduled_date ASC
			ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS planned_delivery_time
	FROM [presentation].[mng_fact_otif_sap]
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
		,dim_date.fiscal_year
		,dim_date.fiscal_period
		,unrestricted_inventory AS starting_inventory
		,ROW_NUMBER() OVER (PARTITION BY material_no order by inv.date DESC) rnk 
	FROM [PowerBI].[presentation].[moe_fact_inv] AS inv
	JOIN dim_date 
		ON inv.date = dim_date.date 
		AND dim_date.is_first_day_of_period = 1
	WHERE [material_no] IS NOT NULL 
		AND YEAR(inv.[date]) >= 2025
)
,plan_last_doc_date AS (
	SELECT 
		site,
		MAX(doc_date) AS last_date
	FROM dwh.projection_production_plan
	GROUP BY site
)
,forecast_last_doc_date AS (
	SELECT 
		MAX(doc_date) AS date
	FROM dwh.projection_pmn_coid
)
,production_plan AS (
	SELECT 
		production_plan.material_id
		,otif_dim_date.fiscal_year  
		,otif_dim_date.fiscal_period 
		,otif_dim_date.first_day_of_period
		,SUM(production_plan.units_plan) AS units_plan
	FROM dwh.projection_production_plan AS production_plan
	JOIN plan_last_doc_date 
		ON production_plan.site = plan_last_doc_date.site
		AND production_plan.doc_date = plan_last_doc_date.last_date
	LEFT JOIN otif 
		ON production_plan.material_id = otif.material_id
	JOIN dim_date
		ON production_plan.fiscal_year = dim_date.fiscal_year
		AND production_plan.fiscal_period = dim_date.fiscal_period
		AND is_first_day_of_period = 1
	JOIN dim_date AS otif_dim_date
		ON DATEADD(DAY, 15 + ISNULL(ROUND((otif.planned_delivery_time / 5.0) * 7, 0), 0), dim_date.date) = otif_dim_date.date
	GROUP BY production_plan.material_id
		,otif_dim_date.fiscal_year  
		,otif_dim_date.fiscal_period 
		,otif_dim_date.first_day_of_period
)
,production_forecast AS (
	SELECT
		coid.material_id,
		dim_date.fiscal_year,
		dim_date.fiscal_period,
		dim_date.first_day_of_period,
		SUM(coid.order_quantity) AS units_forecast
	FROM [dwh].[projection_pmn_coid] AS coid
	JOIN forecast_last_doc_date 
		ON coid.doc_date = forecast_last_doc_date.date
	LEFT JOIN otif
		ON coid.material_id = CAST(otif.material_id AS VARCHAR)
	JOIN dim_date AS dim_date
		ON DATEADD(DAY, ISNULL(ROUND((otif.planned_delivery_time / 5.0) * 7, 0), 0), coid.bsc_start) = dim_date.date
	GROUP BY coid.material_id, 
		dim_date.fiscal_year, 
		dim_date.fiscal_period,
		dim_date.first_day_of_period
)
,SPI AS (
	SELECT 
		COALESCE(production_plan.material_id, production_forecast.material_id, sales.material_id) AS material_id
		,COALESCE(production_plan.fiscal_year, production_forecast.fiscal_year, sales.fiscal_year) AS fiscal_year
		,COALESCE(production_plan.fiscal_period, production_forecast.fiscal_period, sales.fiscal_period) AS fiscal_period
		,COALESCE(production_plan.first_day_of_period, production_forecast.first_day_of_period, sales.first_day_of_period) AS first_day_of_period
		,ISNULL(sales.units_plan, 0) AS sales_plan
		,ISNULL(sales.units_forecast, 0) AS sales_forecast
		,ISNULL(production_plan.units_plan, 0) AS production_plan
		,ISNULL(production_forecast.units_forecast, 0) AS production_forecast
		,ISNULL(inventory.starting_inventory, 0) AS starting_inventory
	FROM production_plan
	FULL JOIN production_forecast 
		ON production_plan.material_id = production_forecast.material_id
		AND production_plan.fiscal_year = production_forecast.fiscal_year
		AND production_plan.fiscal_period = production_forecast.fiscal_period
	FULL JOIN [presentation].[v_projection_fact_sales] AS sales
		ON COALESCE(production_plan.material_id, production_forecast.material_id) = sales.material_id
		AND COALESCE(production_plan.fiscal_year, production_forecast.fiscal_year) = sales.fiscal_year
		AND COALESCE(production_plan.fiscal_period, production_forecast.fiscal_period) = sales.fiscal_period
	LEFT JOIN inventory
		ON COALESCE(production_plan.material_id, production_forecast.material_id, sales.material_id) = inventory.material_id
		AND COALESCE(production_plan.fiscal_year, production_forecast.fiscal_year, sales.fiscal_year) = inventory.fiscal_year
		AND COALESCE(production_plan.fiscal_period, production_forecast.fiscal_period, sales.fiscal_period) = inventory.fiscal_period
		--AND rnk = 1
)
,inventory_projection AS (
	SELECT 
		material_id
		,fiscal_year
		,fiscal_period
		,first_day_of_period
		,sales_plan
		,SUM(IIF(first_day_of_period >= first_day_of_current_period.date,sales_plan,0)) OVER (
			PARTITION BY material_id, fiscal_year 
			ORDER BY fiscal_period) AS sales_plan_running_sum
		,sales_forecast
		,SUM(IIF(first_day_of_period >= first_day_of_current_period.date,sales_forecast,0)) OVER (
			PARTITION BY material_id, fiscal_year 
			ORDER BY fiscal_period) AS sales_forecast_running_sum
		,production_plan
		,SUM(IIF(first_day_of_period >= first_day_of_current_period.date,production_plan,0)) OVER (
			PARTITION BY material_id, fiscal_year 
			ORDER BY fiscal_period) AS production_plan_running_sum
		,production_forecast
		,SUM(IIF(first_day_of_period >= first_day_of_current_period.date,production_forecast,0)) OVER (
			PARTITION BY material_id, fiscal_year 
			ORDER BY fiscal_period) AS production_forecast_running_sum
		,CASE
			WHEN first_day_of_period < first_day_of_current_period.date THEN starting_inventory
			ELSE MAX(IIF(first_day_of_period >= first_day_of_current_period.date, starting_inventory, NULL)) OVER (PARTITION BY material_id)
		END AS starting_inventory
	FROM SPI
	CROSS JOIN first_day_of_current_period
	--WHERE first_day_of_period >= first_day_of_current_period.date
)
SELECT 
	material_id
	,fiscal_year
	,fiscal_period
	,first_day_of_period
	,sales_plan
	--,sales_plan_running_sum
	,sales_forecast
	--,sales_forecast_running_sum
	,production_plan
	--,production_plan_running_sum
	,production_forecast
	--,production_forecast_running_sum
	,starting_inventory
	,starting_inventory + production_plan_running_sum - sales_plan_running_sum AS inventory_units_plan
	,asp * (starting_inventory + production_plan_running_sum - sales_plan_running_sum) AS inventory_amount_plan
	,starting_inventory + production_forecast_running_sum - sales_forecast_running_sum AS inventory_units_forecast
	,asp * (starting_inventory + production_forecast_running_sum - sales_forecast_running_sum) AS inventory_amount_forecast
FROM inventory_projection 
LEFT JOIN [dwh].[mng_asp] AS mng_asp
		ON inventory_projection.material_id = mng_asp.material
		AND inventory_projection.fiscal_year = YEAR(mng_asp.date)
		AND inventory_projection.fiscal_period  = MONTH(mng_asp.date)
--CROSS JOIN first_day_of_current_period
--WHERE first_day_of_period >= date