WITH 
dim_date AS (
	SELECT 
		date,
		CAST(FiscalYear AS INT) AS fiscal_year,
		CAST(FiscalPeriod AS INT) AS fiscal_period,
		MIN(DATE) OVER (PARTITION BY FiscalYear, FiscalPeriod) AS first_day_of_period,
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
SELECT 
	production_plan.material_id
	,production_plan.fiscal_year AS original_fiscal_year
	,production_plan.fiscal_period AS original_fiscal_period
	,dim_date.first_day_of_period AS original_first_day_of_period
	,DATEADD(DAY, (dim_date.period_days_count / 2) , dim_date.date) AS mid_day_of_period
	,otif.planned_delivery_time AS original_planned_delivery_time
	,CAST(ISNULL(ROUND((otif.planned_delivery_time / 5.0) * 7, 0), 0) AS INT) AS fixed_planned_delivery_time
	,DATEADD(DAY, dim_date.period_days_count + ISNULL(ROUND((otif.planned_delivery_time / 5.0) * 7, 0), 0), dim_date.date) AS otif_date
	,otif_dim_date.first_day_of_period AS otif_first_day_of_period
	,otif_dim_date.fiscal_year AS otif_fiscal_year
	,otif_dim_date.fiscal_period AS otif_fiscal_period 
	,production_plan.units_plan AS production_plan
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
WHERE production_plan.material_id = '5985510'
ORDER BY 3
