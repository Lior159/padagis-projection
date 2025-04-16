--CREATE OR ALTER VIEW [presentation].[v_projection_fact_inventory] AS
WITH 
dim_date AS (
	SELECT 
		date,
		CAST(FiscalYear AS INT) AS fiscal_year,
		CAST(FiscalPeriod AS INT) AS fiscal_period,
		MIN(DATE) OVER(PARTITION BY FiscalYear, FiscalPeriod) AS first_day_of_period,
		(COUNT(DATE) OVER (PARTITION BY FiscalYear, FiscalPeriod) / 2) - 1 AS period_days_count,
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
		date,
		FiscalYear,
		FiscalPeriod 
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
	FROM [PowerBI].[presentation].[moe_fact_inv] AS inv
	JOIN dim_date 
		ON inv.date = dim_date.date 
		AND dim_date.is_first_day_of_period = 1
	WHERE [material_no] IS NOT NULL 
		AND YEAR(inv.[date]) >= YEAR(GETDATE())
)
,plan_last_doc_date AS (
	SELECT 
		MAX(doc_date) AS date
	FROM dwh.projection_production_plan
	WHERE site = 'PMN'
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
		ON production_plan.doc_date = plan_last_doc_date.date
	LEFT JOIN otif 
		ON production_plan.material_id = otif.material_id
	JOIN dim_date
		ON production_plan.fiscal_year = dim_date.fiscal_year
		AND production_plan.fiscal_period = dim_date.fiscal_period
		AND is_first_day_of_period = 1
	JOIN dim_date AS otif_dim_date
		ON DATEADD(DAY, dim_date.period_days_count + ISNULL(ROUND((otif.planned_delivery_time / 5.0) * 7, 0), 0), dim_date.date) = otif_dim_date.date
	WHERE production_plan.material_id IS NOT NULL 
		AND production_plan.material_id NOT LIKE '4%'
		AND production_plan.fiscal_year >= YEAR(GETDATE())
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
		SUM(coid.order_quantity * 0.9) AS units_forecast
	FROM [dwh].[projection_pmn_coid] AS coid
	JOIN forecast_last_doc_date 
		ON coid.doc_date = forecast_last_doc_date.date
	LEFT JOIN otif
		ON coid.material_id = CAST(otif.material_id AS VARCHAR)
	JOIN dim_date
		ON DATEADD(DAY, ISNULL(ROUND((otif.planned_delivery_time / 5.0) * 7, 0), 0), coid.bsc_start) = dim_date.date
	CROSS JOIN first_day_of_current_period
	WHERE YEAR(coid.bsc_start) >= YEAR(GETDATE())
		AND coid.material_id IS NOT NULL 
		AND coid.material_id NOT LIKE '4%'
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
			ORDER BY fiscal_period) AS cumulative_sales_plan
		,sales_forecast
		,SUM(IIF(first_day_of_period >= first_day_of_current_period.date,sales_forecast,0)) OVER (
			PARTITION BY material_id, fiscal_year 
			ORDER BY fiscal_period) AS cumulative_sales_forecast
		,production_plan
		,SUM(IIF(first_day_of_period >= first_day_of_current_period.date,production_plan,0)) OVER (
			PARTITION BY material_id, fiscal_year 
			ORDER BY fiscal_period) AS cumulative_production_plan
		,production_forecast
		,SUM(IIF(first_day_of_period >= first_day_of_current_period.date,production_forecast,0)) OVER (
			PARTITION BY material_id, fiscal_year 
			ORDER BY fiscal_period) AS cumulative_production_forecast
		,CASE
			WHEN first_day_of_period < first_day_of_current_period.date THEN starting_inventory
			ELSE MAX(IIF(first_day_of_period >= first_day_of_current_period.date, starting_inventory, NULL)) OVER (PARTITION BY material_id)
		END AS starting_inventory,
		first_day_of_current_period.Date AS first_day_of_current_period,
		first_day_of_current_period.FiscalPeriod AS current_fiscal_period,
		first_day_of_current_period.FiscalYear AS current_fiscal_year
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
	,CASE
		WHEN first_day_of_period >= first_day_of_current_period THEN starting_inventory
	END AS starting_inventory
	,CASE
		WHEN first_day_of_period = first_day_of_current_period THEN starting_inventory
		WHEN first_day_of_period > first_day_of_current_period THEN 
			LAG(starting_inventory + cumulative_production_forecast - cumulative_sales_forecast) 
			OVER (PARTITION BY material_id ORDER BY fiscal_year, fiscal_period)
	END AS forecast_starting_inventory
	,CASE
		WHEN first_day_of_period = first_day_of_current_period THEN starting_inventory
		WHEN first_day_of_period > first_day_of_current_period THEN 
			LAG(starting_inventory + cumulative_production_plan - cumulative_sales_plan) 
			OVER (PARTITION BY material_id ORDER BY fiscal_year, fiscal_period)
	END AS plan_starting_inventory
	,CASE
		WHEN first_day_of_period >= first_day_of_current_period THEN 
			starting_inventory + cumulative_production_plan - cumulative_sales_plan
		ELSE
			LEAD(starting_inventory, 1) OVER (PARTITION BY material_id ORDER BY fiscal_year, fiscal_period)
	END AS inventory_units_plan
	,CASE
		WHEN first_day_of_period >= first_day_of_current_period THEN 
			asp_logic * (starting_inventory + cumulative_production_plan - cumulative_sales_plan)
		ELSE
			asp_logic * LEAD(starting_inventory, 1) OVER (PARTITION BY material_id ORDER BY fiscal_year, fiscal_period)
	END AS inventory_amount_plan

	,CASE
		WHEN first_day_of_period >= first_day_of_current_period THEN 
			starting_inventory + cumulative_production_forecast - cumulative_sales_forecast
		ELSE
			LEAD(starting_inventory, 1) OVER (PARTITION BY material_id ORDER BY fiscal_year, fiscal_period)
	END AS inventory_units_forecast

	,CASE
		WHEN first_day_of_period >= first_day_of_current_period THEN 
			asp_logic * (starting_inventory + cumulative_production_forecast - cumulative_sales_forecast)
		ELSE
			asp_logic * LEAD(starting_inventory, 1) OVER (PARTITION BY material_id ORDER BY fiscal_year, fiscal_period)
	END AS inventory_amount_forecast
FROM inventory_projection 
LEFT JOIN [dwh].[mng_asp] AS mng_asp
		ON inventory_projection.material_id = mng_asp.material
		AND inventory_projection.fiscal_year = YEAR(mng_asp.date)
		AND inventory_projection.fiscal_period  = MONTH(mng_asp.date)
WHERE material_id NOT LIKE '4%' 
--CROSS JOIN first_day_of_current_period
--WHERE first_day_of_period >= date