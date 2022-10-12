from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import time
from airflow.configuration import conf
from statsd import StatsClient



STATSD_HOST = conf.get("metrics", "statsd_host")
STATSD_PORT = conf.get("metrics", "statsd_port")
STATSD_PREFIX = conf.get("metrics", "statsd_prefix")

statsd_client = StatsClient(host=STATSD_HOST, port=STATSD_PORT, prefix=STATSD_PREFIX)

def get_metrics_name_with_tags(metrics_name, customer_id, customer_type):
    return f'{metrics_name},customer_id={customer_id},customer_type={customer_type}'

def job_1_execution_fun():
    print("========> Running Job_1 <=============")
    statsd_client = StatsClient(host=STATSD_HOST, port=STATSD_PORT, prefix=STATSD_PREFIX)
    timer = statsd_client.timer(get_metrics_name_with_tags("Job_1_metrics", "cust_id_1", "Premium")).start()
    time.sleep(2)
    timer.stop()

def job_2_execution_fun():
    print("========> Running Job_2 <=============")
    statsd_client = StatsClient(host=STATSD_HOST, port=STATSD_PORT, prefix=STATSD_PREFIX)
    timer = statsd_client.timer(get_metrics_name_with_tags("Job_2_metrics", "cust_id_1", "Free")).start()
    time.sleep(2)
    timer.stop()

with DAG("simple_dag", start_date=datetime.now(),
         schedule_interval='@daily', catchup=False) as dag:

    Job_1 = PythonOperator(
        task_id="Job_1",
        python_callable=job_1_execution_fun,
        # provide_context=True
    )

    Job_2 = PythonOperator(
        task_id="Job_2",
        python_callable=job_2_execution_fun,
        # provide_context=True
    )

    Job_1 >> Job_2
