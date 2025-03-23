select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select month
from `ambient-scope-454609-d5`.`brazilian_ecommerce`.`dim_date`
where month is null



      
    ) dbt_internal_test