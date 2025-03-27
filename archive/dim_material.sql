CREATE OR ALTER  VIEW [presentation].[v_projection_dim_material] AS
WITH 
production_materials AS (
	SELECT  
		material_id,
		source,
		COUNT(1) OVER (PARTITION BY material_id) AS material_sources_count
	FROM 
		(SELECT DISTINCT 
			material_id,
			pil_material_id,
			LEFT(doc_name, 3) AS source
		FROM [dwh].[projection_production_plan]
		WHERE material_id IS NOT NULL) AS distinct_materials
)
SELECT 
	ndc_name
	,CASE
		WHEN production_materials.material_id IS NULL THEN dim_material.material
		ELSE CONCAT(dim_material.material, '_', production_materials.source) 
	END AS material
	,pil_material_id
	,CASE
		WHEN material_mapping.pil_material_id IS NULL OR production_materials.source = 'PMN'
		THEN 'PMN'
		ELSE 'PIL'
	END AS material_source
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
LEFT JOIN [dwh].[projection_material_id_mapping] AS material_mapping
	ON dim_material.material = material_mapping.material_id
LEFT JOIN production_materials
	ON dim_material.material = production_materials.material_id
	AND production_materials.material_sources_count > 1
GO


