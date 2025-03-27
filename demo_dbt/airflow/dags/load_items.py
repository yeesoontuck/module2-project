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

ORDERITEMS_SCHEMA = [
            {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},                            
            {"name": "order_item_id", "type": "INTEGER", "mode": "NULLABLE"},                      
            {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},                          
            {"name": "seller_id", "type": "STRING", "mode": "NULLABLE"},                           
            {"name": "shipping_limit_date", "type": "DATETIME", "mode": "NULLABLE"},               
            {"name": "price", "type": "NUMERIC", "mode": "NULLABLE"},                              
            {"name": "freight_value", "type": "NUMERIC", "mode": "NULLABLE"},                      
        ]

with DAG("load_items_data", default_args=default_args) as dag:


    load_order_items_data = GCSToBigQueryOperator(
        task_id="load_order_items_data", 
        bucket=GCS_BUCKET,
        source_objects=["data/olist_order_items_dataset.csv"],
        destination_project_dataset_table=f"{DATASET}.olist_order_items_raw",
        schema_fields=ORDERITEMS_SCHEMA,
        source_format="CSV",
        field_delimiter=',',
        skip_leading_rows=1,  # Skip header row
        write_disposition='WRITE_TRUNCATE',  # Replace with 'WRITE_APPEND' if you're adding data
    )

    [load_order_items_data]