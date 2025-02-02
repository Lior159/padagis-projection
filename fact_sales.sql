CREATE OR ALTER VIEW presentation.v_projection_fact_sales AS
WITH 
dim_date AS (
	SELECT DISTINCT
		YearWeekINT
		,FiscalYear
		,FiscalPeriod
		,MIN(Date) OVER (PARTITION BY FiscalYear, FiscalPeriod) AS first_day_of_period
	FROM dwh.general_dim_date_fiscal AS fiscal_weeks
)
,orders AS (
	SELECT 
		ROW_NUMBER() OVER (
			PARTITION BY MATERIAL, ForecastWeek, ForecastYear 
			ORDER BY CAST(FileYear AS INT) DESC, CAST(FilePeriod AS INT) DESC
		) AS row
		,Material AS material_id
		,FiscalPeriod AS fiscal_period
		,ForecastYear AS fiscal_year
		,first_day_of_period
		,ForecastValue AS forecast
	FROM [PowerBI].[dwh].bplan_forecast_release 
	JOIN dim_date
		ON CAST(CONCAT(ForecastYear, IIF(ForecastWeek < 10, '0', ''), ForecastWeek) AS INT) = YearWeekINT
	WHERE Material IS NOT NULL AND ForecastYear = 2025
)
,sales_forecast AS (
	SELECT
		material_id
		,fiscal_period
		,fiscal_year
		,first_day_of_period
		,SUM(forecast) AS units_forecast
		,asp * SUM(forecast) AS amount_forecast
	FROM orders
	LEFT JOIN [dwh].[mng_asp] AS asp
		ON orders.material_id = asp.material
		AND orders.fiscal_year = YEAR(asp.date)
		AND orders.fiscal_period = MONTH(asp.date)
	WHERE row = 1 
	GROUP BY material_id, fiscal_period, fiscal_year, first_day_of_period, asp
)
,weekly_sales_plan AS (
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
	WHERE acount IN ('NORMALIZED_UNITS','NORMALIZED_NET_SALES') AND plan_f.[year] = 2025
)
,agg_sales_plan AS (
	SELECT 
		material_id
		,fiscal_period
		,fiscal_year
		,first_day_of_period
		,NORMALIZED_NET_SALES AS sales
		,NORMALIZED_UNITS AS units
	FROM weekly_sales_plan
	PIVOT(SUM(amount) FOR [acount] IN(NORMALIZED_NET_SALES, NORMALIZED_UNITS)) AS p_t
)
SELECT
	ISNULL(sales_forecast.material_id, agg_sales_plan.material_id) AS material_id
	,ISNULL(sales_forecast.fiscal_period, agg_sales_plan.fiscal_period) AS fiscal_period
	,ISNULL(sales_forecast.fiscal_year, agg_sales_plan.fiscal_year) AS fiscal_year
	,ISNULL(sales_forecast.first_day_of_period, agg_sales_plan.first_day_of_period) AS first_day_of_period
	,agg_sales_plan.units AS units_plan
	,agg_sales_plan.sales AS amount_plan
	,sales_forecast.units_forecast
	,sales_forecast.amount_forecast
FROM sales_forecast
FULL JOIN agg_sales_plan
	ON sales_forecast.material_id = agg_sales_plan.material_id
	AND sales_forecast.fiscal_year = agg_sales_plan.fiscal_year
	AND sales_forecast.fiscal_period = agg_sales_plan.fiscal_period
