from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Parameter specifying the crawl we are downloading
CRAWL = "{{dag_run.conf.get('crawl')}}"
print(CRAWL)

# DB connection parameters
ENDPOINT = "{{dag_run.conf.get('endpoint')}}"
DB = "{{dag_run.conf.get('db')}}"
DBUSER = "{{dag_run.conf.get('user')}}"
DBPASS = "{{dag_run.conf.get('password')}}"

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=60)
}
dag = DAG(
    'pipeline',
    default_args=default_args,
    description='Pipeline defining the e2e processing for the business online app',
    schedule_interval=None,
)

repartition_task = BashOperator(
    task_id='repartition_cc_index',
    bash_command='spark-submit --packages org.apache.hadoop:hadoop-aws:3.2.0 /home/hadoop/business_online/ingestion/repartition_cc_index.py --crawl {}'.format(CRAWL),
    dag=dag,
)

download_tasks = []
crossjoin_tasks = []
for i in range(0, 10):
    download_tasks.append(BashOperator(
        task_id='download_cc_{}'.format(i),
        bash_command=('spark-submit --executor-memory 2G --num-executors 40 --packages '
            'org.apache.hadoop:hadoop-aws:3.2.0 /home/hadoop/business_online/ingestion/download_cc_data.py --crawl {} --bucket {}'
            .format(CRAWL, i)),
        dag=dag,
    ))
    download_tasks[i].set_upstream(repartition_task)

    crossjoin_tasks.append(BashOperator(
        task_id='cross_join_{}'.format(i),
        bash_command=('spark-submit --executor-memory 5G --num-executors 14 --packages '
            'org.apache.hadoop:hadoop-aws:3.2.0 /home/hadoop/business_online/processing/cross_join.py --crawl {} --bucket {}'
            .format(CRAWL, i)),
        dag=dag,
    ))
    crossjoin_tasks[i].set_upstream(download_tasks[i])

mentions_task = BashOperator(
    task_id='mentions',
    bash_command=('spark-submit --executor-memory 5G --num-executors 14 --packages '
        'org.apache.hadoop:hadoop-aws:3.2.0 /home/hadoop/business_online/processing/mentions.py --crawl {}'
        .format(CRAWL)),
    dag=dag,
)
mentions_task.set_upstream(crossjoin_tasks)

sentiment_task = BashOperator(
    task_id='sentiment',
    bash_command=('spark-submit --executor-memory 5G --num-executors 10 --packages '
        'org.apache.hadoop:hadoop-aws:3.2.0 /home/hadoop/business_online/processing/sentiment.py --crawl {}'
        .format(CRAWL)),
    dag=dag,
)
sentiment_task.set_upstream(crossjoin_tasks)

write_to_db_task = BashOperator(
    task_id='write_to_db',
    bash_command=('spark-submit --executor-memory 1G --num-executors 1 --packages '
        'org.apache.hadoop:hadoop-aws:3.2.0 /home/hadoop/business_online/database/write_to_db.py --crawl {} '
        '--endpoint {} --db {} --user {} --password {}'
        .format(CRAWL, ENDPOINT, DB, DBUSER, DBPASS)),
    dag=dag,
)
write_to_db_task.set_upstream([mentions_task, sentiment_task])

