select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_purchase_timestamp
from `ambient-scope-454609-d5`.`brazilian_ecommerce`.`stg_orders`
where order_purchase_timestamp is null



      
    ) dbt_internal_test