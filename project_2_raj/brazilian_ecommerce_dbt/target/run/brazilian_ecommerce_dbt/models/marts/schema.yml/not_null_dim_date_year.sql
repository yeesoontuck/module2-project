select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select year
from `ambient-scope-454609-d5`.`brazilian_ecommerce`.`dim_date`
where year is null



      
    ) dbt_internal_test