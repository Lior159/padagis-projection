--CREATE OR ALTER VIEW [presentation].[v_projection_fact_production] AS
WITH 
first_day_of_current_period AS (
	SELECT 
		date
	FROM dwh.general_dim_date_fiscal 
	WHERE first_day_of_period = 1
	  AND FiscalPeriod = (SELECT FiscalPeriod FROM dwh.general_dim_date_fiscal WHERE date = CAST(GETDATE()-1 AS DATE))
	  AND FiscalYear = (SELECT FiscalYear FROM dwh.general_dim_date_fiscal WHERE date = CAST(GETDATE()-1 AS DATE))
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
		,production_plan.fiscal_period
		,production_plan.fiscal_year
		,dim_date.date AS first_day_of_period
		,production_plan.units_plan
		,production_plan.site
	FROM dwh.projection_production_plan AS production_plan
	JOIN plan_last_doc_date 
		ON production_plan.site = plan_last_doc_date.site
		AND production_plan.doc_date = plan_last_doc_date.last_date
	JOIN dwh.general_dim_date_fiscal AS dim_date
		ON production_plan.fiscal_year = dim_date.FiscalYear
		AND production_plan.fiscal_period = dim_date.FiscalPeriod
		AND [first_day_of_period] = 1
	LEFT JOIN [dwh].[projection_material_id_mapping] AS material_mapping
		ON production_plan.pil_material_id = material_mapping.pil_material_id
		AND production_plan.material_id IS NULL
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
	JOIN [presentation].[general_dim_date_fiscal] AS dim_date
		ON coid.bsc_start = dim_date.date
	--JOIN first_day_of_current_period 
	--	ON coid.doc_date = first_day_of_current_period.date
	WHERE coid.doc_name = 'COID-PIL_20250101_040003.csv'
	GROUP BY material_mapping.material_id, dim_date.FiscalPeriod, dim_date.FiscalYear
)
,pmn_coid AS (
	SELECT
		coid.material_id,
		dim_date.FiscalPeriod AS fiscal_period,
		dim_date.FiscalYear AS fiscal_year,
		SUM(coid.target_qty) AS units_forecast
	FROM [dwh].[projection_pmn_coid] AS coid
	JOIN [presentation].[general_dim_date_fiscal] AS dim_date
		ON coid.bsc_start = dim_date.date
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
SELECT 
	production_plan.material_id
	,production_plan.fiscal_period
	,production_plan.fiscal_year
	,production_plan.first_day_of_period
	,production_plan.units_plan
	,production_forecast.units_forecast
	,production_plan.site
FROM production_plan
LEFT JOIN production_forecast 
	ON production_plan.material_id = production_forecast.material_id
	AND production_plan.fiscal_year = production_forecast.fiscal_year
	AND production_plan.fiscal_period = production_forecast.fiscal_period
	AND production_plan.site = production_forecast.site
