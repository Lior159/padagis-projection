CREATE OR ALTER VIEW [presentation].[v_projection_fact_production] AS
WITH 
first_day_of_current_period AS (
	SELECT 
		date
	FROM dwh.general_dim_date_fiscal 
	WHERE first_day_of_period = 1
		AND CONCAT(FiscalPeriod, FiscalYear) = (SELECT CONCAT(FiscalPeriod, FiscalYear) FROM dwh.general_dim_date_fiscal WHERE date = CAST(GETDATE()-1 AS DATE))
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
		production_plan.material_id
		,production_plan.fiscal_period
		,production_plan.fiscal_year
		,production_plan.units_plan
		,production_plan.site
	FROM dwh.projection_production_plan AS production_plan
	JOIN plan_last_doc_date 
		ON production_plan.site = plan_last_doc_date.site
		AND production_plan.doc_date = plan_last_doc_date.last_date
	WHERE production_plan.material_id IS NOT NULL
		AND production_plan.site = 'PMN'
)
,pmn_coid AS (
	SELECT
		coid.material_id,
		dim_date.FiscalPeriod AS fiscal_period,
		dim_date.FiscalYear AS fiscal_year,
		SUM(coid.order_quantity) AS units_forecast,
		'PMN' AS site
	FROM [dwh].[projection_pmn_coid] AS coid
	JOIN [presentation].[general_dim_date_fiscal] AS dim_date
		ON coid.bsc_start = dim_date.date
	--JOIN first_day_of_current_period 
	--	ON coid.doc_date = first_day_of_current_period.date
	GROUP BY coid.material_id, dim_date.FiscalPeriod, dim_date.FiscalYear
)
SELECT 
	ISNULL(production_plan.material_id, pmn_coid.material_id) AS material_id
	,ISNULL(production_plan.fiscal_year, pmn_coid.fiscal_year) fiscal_year
	,ISNULL(production_plan.fiscal_period, pmn_coid.fiscal_period) AS fiscal_period
	,dim_date.date AS first_day_of_period
	,production_plan.units_plan
	,pmn_coid.units_forecast
	,ISNULL(production_plan.site, pmn_coid.site) AS site
FROM production_plan
FULL JOIN pmn_coid 
	ON production_plan.material_id = pmn_coid.material_id
	AND production_plan.fiscal_year = pmn_coid.fiscal_year
	AND production_plan.fiscal_period = pmn_coid.fiscal_period
	AND production_plan.site = pmn_coid.site
JOIN dwh.general_dim_date_fiscal AS dim_date
	ON ISNULL(production_plan.fiscal_year, pmn_coid.fiscal_year) = dim_date.FiscalYear
	AND ISNULL(production_plan.fiscal_period, pmn_coid.fiscal_period) = dim_date.FiscalPeriod
	AND dim_date.first_day_of_period = 1
