CREATE OR ALTER VIEW presentation.v_projection_sales_plan AS
WITH 
monthly_plan AS (
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
	WHERE acount IN ('NORMALIZED_UNITS','NORMALIZED_NET_SALES')
)
SELECT 
	material_id
	,fiscal_period
	,fiscal_year
	,first_day_of_period
	,NORMALIZED_NET_SALES AS sales
	,NORMALIZED_UNITS AS units
	--,doc_name
	--,CAST(SUBSTRING(doc_date_str,5,2) AS INT) AS doc_period
	--,CAST(LEFT(doc_date_str,4) AS INT) AS doc_year
	--,CAST(CONCAT(LEFT(doc_date_str,4), '-', SUBSTRING(doc_date_str,5,2), '-', RIGHT(doc_date_str,2)) AS DATE) doc_date
FROM monthly_plan
PIVOT(SUM(amount) FOR [acount] IN(NORMALIZED_NET_SALES, NORMALIZED_UNITS)) AS p_t