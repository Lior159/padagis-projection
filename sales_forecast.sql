CREATE OR ALTER VIEW presentation.v_projection_sales_forecast AS
WITH
monthly_forecast AS (
	SELECT
		date_f.Date
  		,forecast.scenario
		,forecast.scenario_period
		,forecast.scenario_version
		,forecast.[period]
		,forecast.[year]
		,forecast.[month]
		,forecast.[fiscal_date_int]  
		,forecast.acount
		,forecast.[material_id]
		,forecast.[amount]
		,CASE 
			WHEN MAX([scenario_period]) OVER (PARTITION BY MONTH) = [scenario_period] THEN 1
			ELSE 0 
		END AS is_forecast
	FROM [PowerBI].[dwh].[commercial_forecast_financials] forecast
	JOIN [presentation].[general_dim_date_fiscal] date_f
		ON forecast.[fiscal_date_int]= date_f.[fiscaldateint] 
	WHERE acount in ('NORMALIZED_UNITS','NORMALIZED_NET_SALES')
)
SELECT 
	Date
	,scenario
	,scenario_period
	,scenario_version
    ,[period]
    ,[year]
    ,[month]
	,CAST([fiscal_date_int] AS VARCHAR) AS fiscal_date_int
    ,NORMALIZED_NET_SALES AS sales
	,NORMALIZED_UNITS AS units
    ,[material_id]
FROM monthly_forecast
PIVOT(SUM(amount) FOR [acount] IN (NORMALIZED_NET_SALES, NORMALIZED_UNITS)) as p_t
WHERE is_forecast = 1 


  
