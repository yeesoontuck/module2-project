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


ORDERS_SCHEMA = [
            {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},                        
            {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},                     
            {"name": "order_status", "type": "STRING", "mode": "NULLABLE"},                    
            {"name": "order_purchase_timestamp", "type": "DATETIME", "mode": "NULLABLE"},      
            {"name": "order_approved_at", "type": "DATETIME", "mode": "NULLABLE"},             
            {"name": "order_delivered_carrier_date", "type": "DATETIME", "mode": "NULLABLE"},  
            {"name": "order_delivered_customer_date", "type": "DATETIME", "mode": "NULLABLE"}, 
            {"name": "order_estimated_delivery_date", "type": "DATETIME", "mode": "NULLABLE"}
        ]

with DAG("load_orders_data", default_args=default_args) as dag:


    load_orders_data = GCSToBigQueryOperator(
        task_id="load_orders_data", 
        bucket=GCS_BUCKET,
        source_objects=["data/olist_orders_dataset.csv"],
        destination_project_dataset_table=f"{DATASET}.olist_orders_raw",
        schema_fields=ORDERS_SCHEMA,
        source_format="CSV",
        field_delimiter=',',
        skip_leading_rows=1,  # Skip header row
        write_disposition='WRITE_TRUNCATE',  # Replace with 'WRITE_APPEND' if you're adding data
    )

    [load_orders_data]