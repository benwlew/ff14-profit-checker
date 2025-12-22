with
	flat_recipe as (
		select
			r."#" as recipe_id,
			--Number,
			classjob.abbreviation as job,
			--RecipeLevelTable,
			--v4,
			r.Item_Result as result_id,
			r.Amount_Result as result_amount,
			r.Item_Ingredient_0 as ingredient0_id,
			r.Amount_Ingredient_0 as ingredient0_amount,
			r.Item_Ingredient_1 as ingredient1_id,
			r.Amount_Ingredient_1 as ingredient1_amount,
			r.Item_Ingredient_2 as ingredient2_id,
			r.Amount_Ingredient_2 as ingredient2_amount,
			r.Item_Ingredient_3 as ingredient3_id,
			r.Amount_Ingredient_3 as ingredient3_amount,
			r.Item_Ingredient_4 as ingredient4_id,
			r.Amount_Ingredient_4 as ingredient4_amount,
			r.Item_Ingredient_5 as ingredient5_id,
			r.Amount_Ingredient_5 as ingredient5_amount,
			r.Item_Ingredient_6 as ingredient6_id,
			r.Amount_Ingredient_6 as ingredient6_amount,
			r.Item_Ingredient_7 as ingredient7_id,
			r.Amount_Ingredient_7 as ingredient7_amount,
		from
			imported.recipe as r
			left join imported.classjob on r.crafttype = classjob.dohdoljobindex
		where
			r.item_result > 0
			and classjob.classjobcategory = 33
			--DOH
	),
	unpivot_recipe as (
		select
			recipe_id,
			job,
			result_id as item_id,
			result_amount as item_amount,
			'result' as recipe_part
		from
			flat_recipe
		union all
		select
			recipe_id,
			job,
			ingredient0_id as item_id,
			ingredient0_amount as item_amount,
			'ingredient0' as type
		from
			flat_recipe
		union all
		select
			recipe_id,
			job,
			ingredient1_id as item_id,
			ingredient1_amount as item_amount,
			'ingredient1' as type
		from
			flat_recipe
		union all
		select
			recipe_id,
			job,
			ingredient2_id as item_id,
			ingredient2_amount as item_amount,
			'ingredient2' as type
		from
			flat_recipe
		union all
		select
			recipe_id,
			job,
			ingredient3_id as item_id,
			ingredient3_amount as item_amount,
			'ingredient3' as type
		from
			flat_recipe
		union all
		select
			recipe_id,
			job,
			ingredient4_id as item_id,
			ingredient4_amount as item_amount,
			'ingredient4' as type
		from
			flat_recipe
		union all
		select
			recipe_id,
			job,
			ingredient5_id as item_id,
			ingredient5_amount as item_amount,
			'ingredient5' as type
		from
			flat_recipe
		union all
		select
			recipe_id,
			job,
			ingredient6_id as item_id,
			ingredient6_amount as item_amount,
			'ingredient6' as type
		from
			flat_recipe
		union all
		select
			recipe_id,
			job,
			ingredient7_id as item_id,
			ingredient7_amount as item_amount,
			'ingredient7' as type
		from
			flat_recipe
	),
	shop_items as (
select
	distinct
			item as item_id,
			price_mid
from
			imported.gilshopitem
inner join imported.item on
	item."#" = gilshopitem.item
	)
select
	ur.*,
	i."Name" as item_name,
	i.Icon as item_icon,
	s.price_mid as shop_price
from
	unpivot_recipe as ur
left join imported.item as i on
	ur.item_id = i."#"
left join shop_items as s on
	ur.item_id = s.item_id
where
	ur.item_id > 0
	and i.IsUntradable = false
order by
	recipe_id asc,
	recipe_part asc