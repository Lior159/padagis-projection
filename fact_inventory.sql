--CREATE OR ALTER VIEW [presentation].[v_projection_fact_inv] AS
WITH spi AS (
	SELECT 
		sales.material_id,
		sales.fiscal_year,
		sales.fiscal_period,
		sales.first_day_of_period,
		ISNULL(sales.units_plan,0) AS sales_plan,
		SUM(ISNULL(sales.units_plan,0)) OVER (
			PARTITION BY sales.material_id, sales.fiscal_year 
			ORDER BY sales.fiscal_period) AS sales_plan_running_sum,
		ISNULL(sales.units_forecast, 0) AS sales_forecast,
		SUM(ISNULL(sales.units_forecast, 0)) OVER (
			PARTITION BY sales.material_id, sales.fiscal_year 
			ORDER BY sales.fiscal_period) AS sales_forecast_running_sum,
		ISNULL(production.units_plan, 0) AS production_plan,
		SUM(ISNULL(production.units_plan, 0)) OVER (
			PARTITION BY sales.material_id, sales.fiscal_year 
			ORDER BY sales.fiscal_period) AS production_plan_running_sum,
		ISNULL(production.units_forecast, 0) AS production_forecast,
		SUM(ISNULL(production.units_forecast, 0)) OVER (
			PARTITION BY sales.material_id, sales.fiscal_year 
			ORDER BY sales.fiscal_period) AS production_forecast_running_sum,
		ISNULL(inventory.unrestricted_inventory, 0) AS current_inventory
		--CASE
		--	WHEN MIN(sales.fiscal_period) OVER (PARTITION BY sales.material_id, sales.fiscal_year) = sales.fiscal_period
		--	THEN inventory.unrestricted_inventory
		--	ELSE 0
		--END AS current_inventory
	FROM [presentation].[v_projection_fact_sales] AS sales
	JOIN [presentation].[v_projection_fact_inv] AS inventory
		ON sales.material_id = inventory.material_no
	JOIN [presentation].[v_projection_fact_production] AS production
		ON sales.material_id = production.material_id
		AND sales.fiscal_year = production.fiscal_year
		AND sales.fiscal_period = production.fiscal_period
)
SELECT
	material_id,
	fiscal_year,
	fiscal_period,
	sales_plan,
	sales_plan_running_sum,
	--sales_forecast,
	--sales_forecast_running_sum,
	production_plan,
	production_plan_running_sum,
	--production_forecast,
	--production_forecast_running_sum,
	current_inventory,
	current_inventory + production_plan_running_sum - sales_plan_running_sum AS inventoy_plan
FROM spi
