SELECT
	ISNULL(orders_forecast.material_id, sales_plan.material_id) AS material_id,
	ISNULL(orders_forecast.fiscal_year, sales_plan.fiscal_year) AS fiscal_year,
	ISNULL(orders_forecast.fiscal_period, sales_plan.fiscal_period) AS fiscal_period,
	orders_forecast.forecast AS units_forecast,
	sales_plan.sales AS sales_plan,
	sales_plan.units AS units_plan
FROM [presentation].[v_projection_orders_forecast] AS orders_forecast
FULL JOIN [presentation].[v_projection_sales_plan] AS sales_plan
	ON orders_forecast.material_id = sales_plan.material_id
	AND orders_forecast.fiscal_year = sales_plan.fiscal_year
	AND orders_forecast.fiscal_period = sales_plan.fiscal_period



