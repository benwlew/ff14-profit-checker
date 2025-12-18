select
    world."#" as pk,
    world.Name as world,
    dc.Name as dcgroup_name,
    #5 as region
    from imported.world
    left join imported.worlddcgrouptype as dc on world.Region = dc."#"
order by
    pk asc