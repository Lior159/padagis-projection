WITH
dim_date AS (
	SELECT 
		date,
		CAST(FiscalYear AS INT) AS fiscal_year,
		CAST(FiscalPeriod AS INT) AS fiscal_period,
		MIN(DATE) OVER(PARTITION BY FiscalYear, FiscalPeriod) AS first_day_of_period,
		IIF(MIN(DATE) OVER(PARTITION BY FiscalYear, FiscalPeriod) = DATE, 1,0) is_first_day_of_period
	FROM dwh.general_dim_date_fiscal
)
,first_day_of_current_period AS (
	SELECT
		first_day_of_period AS date
	FROM dim_date
	WHERE date = CAST(GETDATE() AS DATE)
)
,obs AS (
	SELECT 
		material_id,
		fiscal_year,
		fiscal_period,
		batch,
		unrestricted AS obsolescence_qty,
		unrestricted_amount AS obsolescence_amount,
		shell_life_exp_date,
		FIRST_VALUE(fiscal_year) 
		OVER (partition by material_id, batch ORDER BY fiscal_year, fiscal_period ASC
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS obsolescence_fiscal_year,
		FIRST_VALUE(fiscal_period) 
		OVER (partition by material_id, batch ORDER BY fiscal_year, fiscal_period ASC
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS obsolescence_fiscal_period
	FROM [presentation].[mng_fact_si_mb52] AS si
	JOIN first_day_of_current_period
		ON si.si_date = first_day_of_current_period.date
	JOIN dim_date 
		ON dim_date.is_first_day_of_period = 1
		AND fiscal_year >= 2025
		AND dim_date.Date >= first_day_of_current_period.date
	WHERE unrestricted > 0
	AND DATEDIFF(MONTH, dim_date.first_day_of_period, shell_life_exp_date) <= 12
)
,agg_obs AS (
	SELECT 
		material_id,
		fiscal_year,
		fiscal_period,
		CAST(SUM(obsolescence_qty) AS INT) AS obsolescence_qty,
		CAST(SUM(obsolescence_amount) AS INT) AS obsolescence_amount
	FROM OBS
	WHERE fiscal_year = obsolescence_fiscal_year
		AND fiscal_period = obsolescence_fiscal_period
	GROUP BY material_id, fiscal_year, fiscal_period
)
,inv_obs AS (
	SELECT 
		inv.material_id
		,inv.fiscal_year
		,inv.fiscal_period
		,first_day_of_period
		,first_day_of_current_period.date AS first_day_of_current_period
		,starting_inventory
		,IIF(first_day_of_period >= first_day_of_current_period.date, sales_plan, NULL) AS sales_plan
		,IIF(first_day_of_period >= first_day_of_current_period.date, production_plan, NULL) AS production_plan
		,IIF(first_day_of_period >= first_day_of_current_period.date, sales_forecast, NULL) AS sales_forecast
		,IIF(first_day_of_period >= first_day_of_current_period.date, production_forecast, NULL) AS production_forecast
		,inventory_units_plan AS inventory_plan
		,inventory_units_forecast AS inventory_forecast
		,MAX(IIF(obsolescence_qty IS NOT NULL, inv.fiscal_period, NULL)) OVER (
			PARTITION BY inv.material_id 
			ORDER BY inv.fiscal_year, inv.fiscal_period 
			ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS last_obs_period
		,obsolescence_qty
		,obsolescence_amount
	FROM [presentation].[v_projection_fact_inventory] AS inv
	LEFT JOIN agg_obs
		ON inv.material_id = agg_obs.material_id
		AND inv.fiscal_year = agg_obs.fiscal_year
		AND inv.fiscal_period = agg_obs.fiscal_period
	CROSS JOIN first_day_of_current_period
)
,inv_obs_2 AS (
	SELECT 
		material_id
		,fiscal_year
		,fiscal_period
		,first_day_of_period
		,first_day_of_current_period
		,starting_inventory
		,sales_plan
		,production_plan
		,inventory_plan
		,sales_forecast
		,production_forecast
		,inventory_forecast
		,last_obs_period
		,CASE
			WHEN obsolescence_qty IS NOT NULL THEN
				SUM(sales_plan) OVER (
					PARTITION BY material_id 
					ORDER BY fiscal_year, fiscal_period 
					ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
				) 
		END AS cumulative_sales_plan
		,CASE
			WHEN obsolescence_qty IS NOT NULL THEN
				SUM(sales_forecast) OVER (
					PARTITION BY material_id 
					ORDER BY fiscal_year, fiscal_period 
					ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
				) 
		END AS cumulative_sales_forecast
		,obsolescence_qty
		,CASE
			WHEN obsolescence_qty IS NOT NULL THEN
				SUM(obsolescence_qty) OVER (
					PARTITION BY material_id 
					ORDER BY fiscal_year, fiscal_period 
				) 
		END AS cumulative_obsolescence_qty
		,obsolescence_amount
	FROM inv_obs
)
SELECT 
	material_id
	,fiscal_year
	,fiscal_period
	,first_day_of_period
	,starting_inventory
	,sales_plan
	,production_plan
	,CASE
		WHEN first_day_of_period < first_day_of_current_period THEN inventory_plan
		ELSE SUM(
			IIF(first_day_of_period >= first_day_of_current_period, 
				ISNULL(starting_inventory, 0) + production_plan - sales_plan - ISNULL(obsolescence_qty, 0) + ISNULL(cumulative_sales_plan, 0), 
				0)) 
			OVER (PARTITION BY material_id ORDER BY fiscal_year, fiscal_period)
	END AS inventoy_plan
	,sales_forecast
	,production_forecast 
	,CASE
		WHEN first_day_of_period < first_day_of_current_period THEN inventory_forecast
		ELSE SUM(
			IIF(first_day_of_period >= first_day_of_current_period, 
				ISNULL(starting_inventory, 0) + production_forecast - sales_forecast - ISNULL(obsolescence_qty, 0) + ISNULL(cumulative_sales_forecast, 0), 
				0)) 
				OVER (PARTITION BY material_id ORDER BY fiscal_year, fiscal_period)
	END AS inventoy_forecast
	,last_obs_period
	,cumulative_sales_plan
	,cumulative_sales_plan - [dbo].[min_number](cumulative_sales_plan, cumulative_obsolescence_qty) AS rolling_cumulative_sales_plan
	,cumulative_sales_forecast
	,obsolescence_qty
	,cumulative_obsolescence_qty
	,cumulative_obsolescence_qty - [dbo].[min_number](cumulative_sales_plan, cumulative_obsolescence_qty) AS rolling_cumulative_obsolescence_qty
	,obsolescence_amount
FROM inv_obs_2
WHERE material_id = '5000403' 
--WHERE material_id IN ('5000403', '5005302' )
ORDER BY
	material_id,
	fiscal_year,
	fiscal_period

