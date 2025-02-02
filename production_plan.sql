CREATE OR ALTER VIEW presentation.v_projection_production AS 
WITH last_doc AS (
	SELECT 
		CONCAT(pil_material_id, material_id) AS id
		,fiscal_year
		,fiscal_period
		,MAX(doc_date) AS max_doc_date
	FROM dwh.projection_production_plan
	GROUP BY CONCAT(pil_material_id, material_id), fiscal_year, fiscal_period
)
SELECT 
	material_id
	,production_plan.fiscal_period
	,production_plan.fiscal_year
	,date AS first_day_of_period
	,units_plan
	,100 AS units_forecast
	,LEFT(doc_name, 3) AS source
FROM dwh.projection_production_plan AS production_plan
JOIN last_doc 
	ON CONCAT(production_plan.pil_material_id, production_plan.material_id) = last_doc.id
	AND production_plan.fiscal_year = last_doc.fiscal_year
	AND production_plan.fiscal_period = last_doc.fiscal_period
	AND production_plan.doc_date = last_doc.max_doc_date 
JOIN dwh.general_dim_date_fiscal AS dim_date
	ON production_plan.fiscal_year = dim_date.FiscalYear
	AND production_plan.fiscal_period = dim_date.FiscalPeriod
	AND [first_day_of_period] = 1
WHERE material_id IS NOT NULL

