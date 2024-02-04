from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# define arg
default_args = {
    'owner': 'Nagi A',
    'start_date': days_ago(0),
    'email': ['nagi192@gmail.com'],  
    'email_on_failure': True,  

    
'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#define DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

#task 1
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging',
    dag=dag,
)
#task 2
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d "," -f 1,2,3,4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > 
    /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag,
)
#task 3
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d "\t" -f5,6,7 < /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv | tr "\t" "," > 
    /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag,
)

#task 4
extract_data_from_fixed_width = BashOperator(
    task_id='extract_fixed_width',
    bash_command='cut -d " " -f 6,7 < /home/project/airflow/dags/finalassignment/staging/payment-data.txt | tr " " "," >
     /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    dag=dag,
)
#task 5
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d "," /home/project/airflow/dags/finalassignment/staging/csv_data.csv
     /home/project/airflow/dags/finalassignment/staging/tsv_data.csv
      /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv',
    dag=dag,
)
#task 6 
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='cut -d "," -f 4 < /home/project/airflow/dags/finalassignment/staging/extracted_data.csv | tr [a-z] [A-Z] >
   /home/project/airflow/dags/finalassignment/staging/tranformed_data.csv',
    dag=dag,
)
#task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data