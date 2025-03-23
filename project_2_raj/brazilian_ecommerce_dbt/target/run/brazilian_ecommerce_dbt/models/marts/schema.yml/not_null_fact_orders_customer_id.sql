select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_id
from `ambient-scope-454609-d5`.`brazilian_ecommerce`.`fact_orders`
where customer_id is null



      
    ) dbt_internal_test