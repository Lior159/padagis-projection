CREATE OR ALTER VIEW presentation.v_projection_orders_forecast AS
WITH 
dim_date AS (
	SELECT DISTINCT
		YearWeekINT,
		FiscalYear,
		FiscalPeriod,
		MIN(Date) OVER (PARTITION BY FiscalYear, FiscalPeriod) AS first_day_of_period
	FROM dwh.general_dim_date_fiscal AS fiscal_weeks
)
,ranked_table AS (
	SELECT 
		ROW_NUMBER() OVER (
			PARTITION BY MATERIAL, ForecastWeek, ForecastYear 
			ORDER BY CAST(FileYear AS INT) DESC, CAST(FilePeriod AS INT) DESC
		) AS row,
		Material AS material_id,
		FiscalPeriod AS fiscal_period,
		ForecastYear AS fiscal_year,
		first_day_of_period,
		ForecastValue AS forecast
	FROM [PowerBI].[dwh].bplan_forecast_release 
	JOIN dim_date
		ON CAST(CONCAT(ForecastYear, IIF(ForecastWeek < 10, '0', ''), ForecastWeek) AS INT) = YearWeekINT
	WHERE Material IS NOT NULL
)
SELECT
	material_id,
	fiscal_period,
	fiscal_year,
	first_day_of_period,
	SUM(forecast) AS forecast
FROM ranked_table
WHERE row = 1 
GROUP BY material_id,
	fiscal_period,
	fiscal_year,
	first_day_of_period

