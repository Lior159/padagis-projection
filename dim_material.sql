CREATE OR ALTER  VIEW [presentation].[v_projection_dim_material] AS
WITH 
production_materials AS (
	SELECT DISTINCT
		material_id
	FROM [presentation].[v_projection_fact_production]
)
SELECT 
	ndc_name
	,material
	,NULL AS pil_material_id
	,NULL AS material_source
	,RevisedNDCUnique
	,material_desc
	,ndc
	,RevisedNDC
	,product_hierarchy_level_1
	,product_hierarchy_level_2
	,product_hierarchy_level_3
	,bplan_product_category
	,bplan_product_subcategory
	,bplan_product_family
	,bplan_segment
	,bplan_product_catgory_groups
	,value_stream
	,is_made_to_order
	,material_status
	,material_category
FROM [presentation].[moe_dim_material] AS dim_material
JOIN production_materials
	ON dim_material.material = production_materials.material_id
GO


