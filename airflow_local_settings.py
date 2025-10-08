"""
Custom Airflow configuration to clean up log file paths
"""

# Override the default log filename template to remove the dag_id= and run_id= prefixes
LOG_FILENAME_TEMPLATE = "{{ ti.dag_id }}/{{ ti.run_id }}/{{ ti.task_id }}/{% if ti.map_index >= 0 %}{{ ti.map_index }}/{% endif %}{{ ti.try_number }}.log"

