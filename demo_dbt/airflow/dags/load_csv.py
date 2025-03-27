from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['yeesoontuck@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
GCS_BUCKET = 'us-central1-project2-compos-5c62fdca-bucket'
DATASET = 'concise-faculty-452613-a1.composer_test'

CUSTOMER_SCHEMA = [
            {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_unique_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_zip_code_prefix", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_state", "type": "STRING", "mode": "NULLABLE"}
        ]

PRODUCT_SCHEMA = [
            {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_category_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_name_length", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_description_length", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_photos_qty", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_weight_g", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_length_cm", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_height_cm", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_width_cm", "type": "INTEGER", "mode": "NULLABLE"}
        ]

CATEGORY_SCHEMA = [
            {"name": "product_category_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_category_name_english", "type": "STRING", "mode": "NULLABLE"},
        ]



PAYMENTS_SCHEMA = [
            {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},                           
            {"name": "payment_sequential", "type": "INTEGER", "mode": "NULLABLE"},                
            {"name": "payment_type", "type": "STRING", "mode": "NULLABLE"},                       
            {"name": "payment_installments", "type": "INTEGER", "mode": "NULLABLE"},              
            {"name": "payment_value", "type": "NUMERIC", "mode": "NULLABLE"},                     
]



with DAG("load_csv_data", default_args=default_args) as dag:

    load_customer_data = GCSToBigQueryOperator(
        task_id="load_customer_data", 
        bucket=GCS_BUCKET,
        source_objects=["data/olist_customers_dataset.csv"],
        destination_project_dataset_table=f"{DATASET}.olist_customers_raw",
        schema_fields=CUSTOMER_SCHEMA,
        source_format="CSV",
        field_delimiter=',',
        skip_leading_rows=1,  # Skip header row
        write_disposition='WRITE_TRUNCATE',  # Replace with 'WRITE_APPEND' if you're adding data
    )

    load_product_data = GCSToBigQueryOperator(
        task_id="load_product_data", 
        bucket=GCS_BUCKET,
        source_objects=["data/olist_products_dataset.csv"],
        destination_project_dataset_table=f"{DATASET}.olist_products_raw",
        schema_fields=PRODUCT_SCHEMA,
        source_format="CSV",
        field_delimiter=',',
        skip_leading_rows=1,  # Skip header row
        write_disposition='WRITE_TRUNCATE',  # Replace with 'WRITE_APPEND' if you're adding data
    )

    load_product_category_names_data = GCSToBigQueryOperator(
        task_id="load_product_category_names_data", 
        bucket=GCS_BUCKET,
        source_objects=["data/product_category_name_translation.csv"],
        destination_project_dataset_table=f"{DATASET}.olist_order_product_category_name_translation_raw",
        schema_fields=CATEGORY_SCHEMA,
        source_format="CSV",
        field_delimiter=',',
        skip_leading_rows=1,  # Skip header row
        write_disposition='WRITE_TRUNCATE',  # Replace with 'WRITE_APPEND' if you're adding data
    )

    load_payments_data = GCSToBigQueryOperator(
        task_id="load_payments_data", 
        bucket=GCS_BUCKET,
        source_objects=["data/olist_order_payments_dataset.csv"],
        destination_project_dataset_table=f"{DATASET}.olist_order_payments_raw",
        schema_fields=PAYMENTS_SCHEMA,
        source_format="CSV",
        field_delimiter=',',
        skip_leading_rows=1,  # Skip header row
        write_disposition='WRITE_TRUNCATE',  # Replace with 'WRITE_APPEND' if you're adding data
    )

    # compute_revenue = BigQueryOperator(
    #     task_id="compute_revenue",
    #     sql="""
    #         SELECT p.product_id, p.name, SUM(s.quantity * p.price) AS revenue
    #         FROM `{DATASET}.products` p
    #         JOIN `{DATASET}.sales` s ON p.product_id = s.product_id
    #         GROUP BY p.product_id, p.name
    #     """,
    #     destination_dataset_table=f"{DATASET}.product_revenue", 
    #     write_disposition="WRITE_TRUNCATE",
    # )

    # [load_product_data, load_sales_data] >> compute_revenue
    [load_customer_data, load_product_data, load_product_category_names_data, load_payments_data]