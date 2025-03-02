"""
    # Task 3: Trigger the transformation DAG after ingestion is complete
    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="dbt_uber_transformations_dag",
    )
    create_raw_schema >> load_csv >> trigger_transform_dag
    
"""