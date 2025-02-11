CREATE OR ALTER VIEW [presentation].[v_projection_fact_production] AS
WITH
production_materials AS (
	SELECT  
		material_id
	FROM 
		(SELECT DISTINCT 
			material_id,
			LEFT(doc_name, 3) AS source
		FROM [dwh].[projection_production_plan]
		WHERE material_id IS NOT NULL) AS distinct_materials
	GROUP BY material_id
	HAVING COUNT(1) > 1
)
,last_doc AS (
	SELECT 
		CONCAT(pil_material_id, material_id) AS id
		,fiscal_year
		,fiscal_period
		,MAX(doc_date) AS max_doc_date
	FROM dwh.projection_production_plan
	GROUP BY CONCAT(pil_material_id, material_id), fiscal_year, fiscal_period
)
,prod_plan AS (
	SELECT 
		material_id
		,production_plan.fiscal_period
		,production_plan.fiscal_year
		,date AS first_day_of_period
		,units_plan
		,LEFT(doc_name, 3) AS source
	FROM dwh.projection_production_plan AS production_plan
	JOIN last_doc 
		ON CONCAT(production_plan.pil_material_id, production_plan.material_id) = last_doc.id
		AND production_plan.fiscal_year = last_doc.fiscal_year
		AND production_plan.fiscal_period = last_doc.fiscal_period
		AND max_doc_date = doc_date
	JOIN dwh.general_dim_date_fiscal AS dim_date
		ON production_plan.fiscal_year = dim_date.FiscalYear
		AND production_plan.fiscal_period = dim_date.FiscalPeriod
		AND [first_day_of_period] = 1
	WHERE material_id IS NOT NULL
)
,pil_coid AS (
	SELECT
		material_mapping.material_id,
		dim_date.FiscalPeriod AS fiscal_period,
		dim_date.FiscalYear AS fiscal_year,
		SUM(coid.target_qty) AS units_forecast
	FROM [dwh].[projection_material_id_mapping] AS material_mapping
	LEFT JOIN [dwh].[projection_pil_coid] AS coid
		ON material_mapping.pil_material_id = coid.material_id 
	JOIN [presentation].[general_dim_date_fiscal] AS dim_date
		ON coid.bsc_start = dim_date.date
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
	GROUP BY coid.material_id, dim_date.FiscalPeriod, dim_date.FiscalYear
)
,prod_forecast AS (
	SELECT
		material_id,
		fiscal_period,
		fiscal_year,
		units_forecast,
		'PIL' AS source
	FROM pil_coid
	UNION ALL 
	SELECT
		material_id,
		fiscal_period,
		fiscal_year,
		units_forecast,
		'PMN' AS source
	FROM pmn_coid
)
SELECT 
	CASE
		WHEN production_materials.material_id IS NULL THEN prod_plan.material_id
		ELSE CONCAT(prod_plan.material_id, '_', prod_plan.source) 
	END AS material_id
	,prod_plan.fiscal_period
	,prod_plan.fiscal_year
	,prod_plan.first_day_of_period
	,prod_plan.units_plan
	,prod_forecast.units_forecast
	,prod_plan.source
FROM prod_plan
LEFT JOIN prod_forecast
	ON prod_plan.material_id = prod_forecast.material_id
	AND prod_plan.fiscal_year = prod_forecast.fiscal_year
	AND prod_plan.fiscal_period = prod_forecast.fiscal_period
	AND prod_plan.source = prod_forecast.source
LEFT JOIN production_materials
	ON prod_plan.material_id = production_materials.material_id
