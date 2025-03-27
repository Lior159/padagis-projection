CREATE OR ALTER VIEW [presentation].[v_projection_fact_production] AS
WITH 
first_day_of_current_period AS (
	SELECT 
		date
	FROM dwh.general_dim_date_fiscal 
	WHERE first_day_of_period = 1
		AND CONCAT(FiscalPeriod, FiscalYear) = (SELECT CONCAT(FiscalPeriod, FiscalYear) FROM dwh.general_dim_date_fiscal WHERE date = CAST(GETDATE() AS DATE))
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
		,production_plan.fiscal_period
		,production_plan.fiscal_year
		,production_plan.units_plan
	FROM dwh.projection_production_plan AS production_plan
	JOIN plan_last_doc_date 
		ON production_plan.doc_date = plan_last_doc_date.date
	WHERE production_plan.material_id IS NOT NULL 
		AND production_plan.fiscal_year >= 2025
)
,pmn_coid AS (
	SELECT
		coid.material_id,
		dim_date.FiscalPeriod AS fiscal_period,
		dim_date.FiscalYear AS fiscal_year,
		SUM(IIF(coid.bsc_start >= first_day_of_current_period.date, coid.order_quantity * 0.9, NULL)) AS units_forecast,
		SUM(IIF(coid.bsc_start < first_day_of_current_period.date AND coid.act_finish_date > coid.bsc_start, coid.del_quantity, NULL)) AS units_actual
	FROM [dwh].[projection_pmn_coid] AS coid
	JOIN forecast_last_doc_date 
		ON coid.doc_date = forecast_last_doc_date.date
	JOIN [presentation].[general_dim_date_fiscal] AS dim_date
		ON coid.bsc_start = dim_date.date
	CROSS JOIN first_day_of_current_period 
	WHERE dim_date.FiscalYear >= 2025
	GROUP BY coid.material_id, dim_date.FiscalPeriod, dim_date.FiscalYear
)
SELECT 
	ISNULL(production_plan.material_id, pmn_coid.material_id) AS material_id
	,ISNULL(production_plan.fiscal_year, pmn_coid.fiscal_year) fiscal_year
	,ISNULL(production_plan.fiscal_period, pmn_coid.fiscal_period) AS fiscal_period
	,dim_date.date AS first_day_of_period
	,production_plan.units_plan
	,pmn_coid.units_forecast
	,pmn_coid.units_actual
	,production_plan.units_plan * asp AS amount_plan
	,pmn_coid.units_forecast * asp AS amount_forecast
	,pmn_coid.units_actual * asp AS amount_actual
FROM production_plan
FULL JOIN pmn_coid 
	ON production_plan.material_id = pmn_coid.material_id
	AND production_plan.fiscal_year = pmn_coid.fiscal_year
	AND production_plan.fiscal_period = pmn_coid.fiscal_period
LEFT JOIN [dwh].[mng_asp] AS mng_asp
		ON ISNULL(production_plan.material_id, pmn_coid.material_id) = mng_asp.material
		AND ISNULL(production_plan.fiscal_year, pmn_coid.fiscal_year) = YEAR(mng_asp.date)
		AND ISNULL(production_plan.fiscal_period, pmn_coid.fiscal_period) = MONTH(mng_asp.date)
JOIN dwh.general_dim_date_fiscal AS dim_date
	ON ISNULL(production_plan.fiscal_year, pmn_coid.fiscal_year) = dim_date.FiscalYear
	AND ISNULL(production_plan.fiscal_period, pmn_coid.fiscal_period) = dim_date.FiscalPeriod
	AND dim_date.first_day_of_period = 1
