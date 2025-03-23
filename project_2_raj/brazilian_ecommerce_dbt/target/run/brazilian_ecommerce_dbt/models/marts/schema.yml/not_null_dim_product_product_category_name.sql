select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select product_category_name
from `ambient-scope-454609-d5`.`brazilian_ecommerce`.`dim_product`
where product_category_name is null



      
    ) dbt_internal_test