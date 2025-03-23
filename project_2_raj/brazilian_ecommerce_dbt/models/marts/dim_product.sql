SELECT DISTINCT
  product_id,
  product_category_name,
  product_name_lenght,
  product_description_lenght,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM {{ source('ecommerce_brazil', 'products_dataset') }}
