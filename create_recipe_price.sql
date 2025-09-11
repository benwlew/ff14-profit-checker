with
shop as(
select distinct item, price_mid
from imported.gilshopitem
inner join imported.item on item."#" = gilshopitem.item),
recipe_price as(
select recipe."#" as pk,
--Number,
--CraftType,
--RecipeLevelTable,
--v4,
Item_Result as result_id,
i.name as result_name,
s.price_mid as result_shop_price,
Amount_Result as result_amount,
Item_Ingredient_0 as ingredient0_id,
i0.name as ingredient0_name,
s0.price_mid as ingredient0_shop_price,
Amount_Ingredient_0 as ingredient0_amount,
Item_Ingredient_1 as ingredient1_id,
i1.name as ingredient1_name,
s1.price_mid as ingredient1_shop_price,
Amount_Ingredient_1 as ingredient1_amount,
Item_Ingredient_2 as ingredient2_id,
i2.name as ingredient2_name,
s2.price_mid as ingredient2_shop_price,
Amount_Ingredient_2 as ingredient2_amount,
Item_Ingredient_3 as ingredient3_id,
i3.name as ingredient3_name,
s3.price_mid as ingredient3_shop_price,
Amount_Ingredient_3 as ingredient3_amount,
Item_Ingredient_4 as ingredient4_id,
i4.name as ingredient4_name,
s4.price_mid as ingredient4_shop_price,
Amount_Ingredient_4 as ingredient4_amount,
Item_Ingredient_5 as ingredient5_id,
i5.name as ingredient5_name,
s5.price_mid as ingredient5_shop_price,
Amount_Ingredient_5 as ingredient5_amount,
Item_Ingredient_6 as ingredient6_id,
i6.name as ingredient6_name,
s6.price_mid as ingredient6_shop_price,
Amount_Ingredient_6 as ingredient6_amount,
Item_Ingredient_7 as ingredient7_id,
i7.name as ingredient7_name,
s7.price_mid as ingredient7_shop_price,
Amount_Ingredient_7 as ingredient7_amount,
--RecipeNotebookList,
--DisplayPriority,
--IsSecondary,
--MaterialQualityFactor,
--DifficultyFactor,
--QualityFactor,
--DurabilityFactor,
--RequiredQuality,
--RequiredCraftsmanship,
--RequiredControl,
--QuickSynthCraftsmanship,
--QuickSynthControl,
--SecretRecipeBook,
--Quest,
CanQuickSynth,
CanHq,
--ExpRewarded,
--Status_Required,
--Item_Required,
--IsSpecializationRequired,
IsExpert,
--PatchNumber,
from imported.recipe
left join imported.item as i on recipe.item_result <> 0 and recipe.item_result = i."#"
left join imported.item as i0 on recipe.item_ingredient_0 = i0."#"
left join imported.item as i1 on recipe.item_ingredient_1 = i1."#"
left join imported.item as i2 on recipe.item_ingredient_2 = i2."#"
left join imported.item as i3 on recipe.item_ingredient_3 = i3."#"
left join imported.item as i4 on recipe.item_ingredient_4 = i4."#"
left join imported.item as i5 on recipe.item_ingredient_5 = i5."#"
left join imported.item as i6 on recipe.item_ingredient_6 = i6."#"
left join imported.item as i7 on recipe.item_ingredient_7 = i7."#"
left join shop as s on s.item = recipe.item_result
left join shop as s0 on recipe.item_ingredient_0 = s.item
left join shop as s1 on recipe.item_ingredient_1 = s1.item
left join shop as s2 on recipe.item_ingredient_2 = s2.item
left join shop as s3 on recipe.item_ingredient_3 = s3.item
left join shop as s4 on recipe.item_ingredient_4 = s4.item
left join shop as s5 on recipe.item_ingredient_5 = s5.item
left join shop as s6 on recipe.item_ingredient_6 = s6.item
left join shop as s7 on recipe.item_ingredient_7 = s7.item
where recipe.item_result <> 0
--where s.price_mid is not null
--where s0.price_mid is not null
)
select * from recipe_price
order by pk asc