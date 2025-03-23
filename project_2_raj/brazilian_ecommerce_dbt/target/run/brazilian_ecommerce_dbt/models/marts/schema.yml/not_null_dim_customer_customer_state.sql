select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_state
from `ambient-scope-454609-d5`.`brazilian_ecommerce`.`dim_customer`
where customer_state is null



      
    ) dbt_internal_test