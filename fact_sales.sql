CREATE OR ALTER VIEW presentation.v_projection_fact_sales AS
WITH 
materials AS (
	SELECT DISTINCT 
		material_id 
	FROM [presentation].[v_projection_fact_production]
)
,first_day_of_current_period AS (
	SELECT 
		date,
		CAST(FiscalYear AS INT) AS FiscalYear,
		CAST(FiscalPeriod AS INT) AS FiscalPeriod
	FROM dwh.general_dim_date_fiscal 
	WHERE first_day_of_period = 1
		AND CONCAT(FiscalPeriod, FiscalYear) = (SELECT CONCAT(FiscalPeriod, FiscalYear) FROM dwh.general_dim_date_fiscal WHERE date = CAST(GETDATE() AS DATE))
)
,dim_date AS (
	SELECT DISTINCT
		Date
		,YearWeekINT
		,CAST(FiscalYear AS INT) AS FiscalYear 
		,CAST(FiscalPeriod AS INT) AS FiscalPeriod
		,MIN(Date) OVER (PARTITION BY FiscalYear, FiscalPeriod) AS first_day_of_period
	FROM dwh.general_dim_date_fiscal AS fiscal_weeks
)
,bplan AS (
	SELECT 
		ROW_NUMBER() OVER (
			PARTITION BY bplan.MATERIAL, bplan.ForecastWeek, bplan.ForecastYear 
			ORDER BY CAST(FileYear AS INT) DESC, CAST(FilePeriod AS INT) DESC
		) AS row
		,bplan.Material AS material_id
		,dim_date.FiscalPeriod AS fiscal_period
		,bplan.ForecastYear AS fiscal_year
		,dim_date.first_day_of_period
		,bplan.ForecastValue AS forecast
	FROM [PowerBI].[dwh].bplan_forecast_release AS bplan
	JOIN dim_date
		ON CAST(CONCAT(ForecastYear, IIF(ForecastWeek < 10, '0', ''), ForecastWeek) AS INT) = YearWeekINT
	CROSS JOIN first_day_of_current_period
	WHERE Material IS NOT NULL 
		AND bplan.ForecastYear >= YEAR(GETDATE())
)
,sales_forecast AS (
	SELECT
		material_id
		,fiscal_period
		,fiscal_year
		,first_day_of_period
		,SUM(forecast) AS units_forecast
		,asp_logic * SUM(forecast) AS amount_forecast
	FROM bplan
	LEFT JOIN [dwh].[mng_asp] AS mng_asp
		ON bplan.material_id = mng_asp.material
		AND bplan.fiscal_year = YEAR(mng_asp.date)
		AND bplan.fiscal_period = MONTH(mng_asp.date)
	WHERE row = 1 
	GROUP BY material_id, fiscal_period, fiscal_year, first_day_of_period, asp_logic
)
,commercial_plan AS (
	SELECT 
		plan_f.[material_id]
		,plan_f.[month] AS fiscal_period
		,plan_f.[year] AS fiscal_year
		,date_f.date AS first_day_of_period
		,plan_f.acount
		,plan_f.[amount]
	FROM [PowerBI].[dwh].[commercial_plan_financials] AS plan_f
	JOIN [presentation].[general_dim_date_fiscal] date_f
		ON plan_f.fiscal_date_int= date_f.fiscaldateint
	WHERE acount IN ('NORMALIZED_UNITS','NORMALIZED_NET_SALES') AND plan_f.[year] >= YEAR(GETDATE())
)
,sales_plan AS (
	SELECT 
		material_id
		,fiscal_period
		,fiscal_year
		,first_day_of_period
		,NORMALIZED_NET_SALES AS sales
		,NORMALIZED_UNITS AS units
	FROM commercial_plan
	PIVOT(SUM(amount) FOR [acount] IN(NORMALIZED_NET_SALES, NORMALIZED_UNITS)) AS p_t
)
,commercial_actual AS (
	SELECT 
		plan_f.[material_id]
		,plan_f.[month] AS fiscal_period
		,plan_f.[year] AS fiscal_year
		,date_f.date AS first_day_of_period
		,plan_f.acount
		,plan_f.[amount]
	FROM [PowerBI].[dwh].[commercial_actual_financials] AS plan_f
	JOIN [presentation].[general_dim_date_fiscal] date_f
		ON plan_f.fiscal_date_int= date_f.fiscaldateint
	WHERE acount IN ('NORMALIZED_UNITS','NORMALIZED_NET_SALES') AND plan_f.[year] >= YEAR(GETDATE())
)
,sales_actual AS (
	SELECT 
		material_id
		,fiscal_period
		,fiscal_year
		,first_day_of_period
		,NORMALIZED_NET_SALES AS sales
		,NORMALIZED_UNITS AS units
	FROM commercial_actual
	PIVOT(SUM(amount) FOR [acount] IN(NORMALIZED_NET_SALES, NORMALIZED_UNITS)) AS p_t
)
SELECT 
	COALESCE(sales_forecast.material_id, sales_plan.material_id, sales_actual.material_id) AS material_id
	,COALESCE(sales_forecast.fiscal_period, sales_plan.fiscal_period, sales_actual.fiscal_period) AS fiscal_period
	,COALESCE(sales_forecast.fiscal_year, sales_plan.fiscal_year, sales_actual.fiscal_year) AS fiscal_year
	,COALESCE(sales_forecast.first_day_of_period, sales_plan.first_day_of_period, sales_actual.first_day_of_period) AS first_day_of_period
	,sales_plan.units AS units_plan
	,COALESCE(sales_actual.units, sales_plan.units) AS units_plan_with_actual
	,sales_plan.sales AS amount_plan
	,COALESCE(sales_actual.sales, sales_plan.sales) AS amount_plan_with_actual
	,sales_forecast.units_forecast
	,COALESCE(sales_actual.units, sales_forecast.units_forecast) AS units_forecast_with_actual
	,sales_forecast.amount_forecast
	,COALESCE(sales_actual.sales, sales_forecast.amount_forecast) AS amount_forecast_with_actual
	,sales_actual.units AS units_actual
	,sales_actual.sales AS amount_actual
FROM sales_forecast
FULL JOIN sales_plan
	ON sales_forecast.material_id = sales_plan.material_id
	AND sales_forecast.fiscal_year = sales_plan.fiscal_year
	AND sales_forecast.fiscal_period = sales_plan.fiscal_period
FULL JOIN sales_actual 
	ON COALESCE(sales_forecast.material_id, sales_plan.material_id) = sales_actual.material_id
	AND COALESCE(sales_forecast.fiscal_year, sales_plan.fiscal_year) = sales_actual.fiscal_year
	AND COALESCE(sales_forecast.fiscal_period, sales_plan.fiscal_period) = sales_actual.fiscal_period
JOIN materials
	ON COALESCE(sales_forecast.material_id, sales_plan.material_id, sales_actual.material_id) = materials.material_id
GO


