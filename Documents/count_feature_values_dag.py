from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator, BigQueryCheckOperator, BigQueryGetDataOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.models import Variable
import airflow
import pytz
import pendulum
import datetime

LOCAL_TZ = pendulum.timezone("Asia/Jakarta")

DEFAULT_CREDENTIALS = Variable.get("DEFAULT_CREDENTIALS", deserialize_json=True)
PROJECT = DEFAULT_CREDENTIALS["PROJECT"]
DATASET_CSS = DEFAULT_CREDENTIALS["DATASET_CSS"]

SCALABILITY_CREDENTIALS = Variable.get("SCALABILITY_CREDENTIALS", deserialize_json=True)
BUCKET_DATAFLOW_LOCATION = SCALABILITY_CREDENTIALS["BUCKET_DATAFLOW_LOCATION"]
MODEL_ID = SCALABILITY_CREDENTIALS["MODEL_ID"]
CALCULATION_TYPE = SCALABILITY_CREDENTIALS["CALCULATION_TYPE"]
VERSION = SCALABILITY_CREDENTIALS["VERSION"]
MISSING_VALUE = SCALABILITY_CREDENTIALS["MISSING_VALUE"]
NUM_WORKER = SCALABILITY_CREDENTIALS["NUM_WORKER"]
MAX_WORKER = SCALABILITY_CREDENTIALS["MAX_WORKER"]
MACHINE_TYPE = SCALABILITY_CREDENTIALS["MACHINE_TYPE"]
SCRIPT_PATH = SCALABILITY_CREDENTIALS["SCRIPT_PATH"]
DF_SUBNET = SCALABILITY_CREDENTIALS["DF_SUBNET"]
CC_SLACK = SCALABILITY_CREDENTIALS["CC_SLACK"]


def execute_slack_notification(context, slack_connection_id, msg, cc_slack):
    slack_msg = """
                {status} : {task} 
                """.format(
        status=msg,
        task=context.get('task_instance').task_id,
    )

    if cc_slack:
        slack_msg = slack_msg + " cc {cc_slack}".format(cc_slack=cc_slack)

    return SlackWebhookOperator(
        task_id='slack_notifications',
        http_conn_id=slack_connection_id,
        webhook_token=BaseHook.get_connection(slack_connection_id).password,
        message=slack_msg,
        username='airflow',
        dag=dag).execute(context=context)


def slack_failed_task(context):
    slack_msg = ":red_circle: Task Failed "
    slack_connection_id = 'slack_mle'
    return execute_slack_notification(context, slack_connection_id, slack_msg, CC_SLACK)


def slack_success_task(context):
    slack_msg = ":large_blue_circle: Task Success"
    slack_connection_id = 'slack_mle'
    return execute_slack_notification(context, slack_connection_id, slack_msg, None)

def query_check_feature_distribution_exists():
    return """SELECT COUNT(*)
    FROM `{PROJECT}.{DATASET_CSS}.feature_distribution`""".format(
        PROJECT=PROJECT,
        DATASET_CSS=DATASET_CSS
    )

def query_check_metrics_scope_value():
    return """ 
    SELECT COUNT(*) FROM `{PROJECT}.{DATASET_CSS}.feature_distribution`
    WHERE DATE(created_at) IN (
        SELECT DATE(created_at) FROM `{PROJECT}.{DATASET_CSS}.feature_distribution`
        WHERE DATE(created_at) BETWEEN (DATE_ADD(CURRENT_DATE(), INTERVAL -6 DAY)) AND CURRENT_DATE()
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 6
    )""".format(
        PROJECT=PROJECT,
        DATASET_CSS=DATASET_CSS
    )

def query_check_feature_distribution_duplicate():
    return """SELECT COUNT(*)
    FROM `{PROJECT}.{DATASET_CSS}.feature_distribution`
    WHERE DATE(created_at) = DATE(CURRENT_TIMESTAMP() , 'Asia/Jakarta')""".format(
        PROJECT=PROJECT,
        DATASET_CSS=DATASET_CSS
    )
    
default_args = {
    'owner': 'MLE-TEAM',
    'depends_on_past': False,
    'email': ['felicya.antoni@finaccel.co'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2022, 3, 17, tzinfo=LOCAL_TZ),
    'on_failure_callback': slack_failed_task,
    'on_success_callback': slack_success_task,
}


with airflow.DAG(
        'alert_ascore_feature',
        default_args=default_args,
        schedule_interval="0 8 * * *"
) as dag:
    local_datetime_now = datetime.datetime.now(pytz.timezone('Asia/Jakarta')).date()
    yesterday_date = local_datetime_now - datetime.timedelta(days=1)

    # checking feature dsitribution table exists
    check_feature_distribution_exists = BigQueryCheckOperator(
        task_id="CHECKING_FEATURE_distribution_TABLE_EXISTS",
        sql=query_check_feature_distribution_exists(),
        use_legacy_sql=False,
        location="asia-southeast2",
    )

    # validate feature table
    pass_validate_feature_table = DummyOperator(
        task_id="PASS_VALIDATE_FEATURE_DISTRIBUTION_TABLE",
        trigger_rule=TriggerRule.ALL_FAILED
    )

    # validate duplicate
    check_duplicate_value = BigQueryValueCheckOperator(
    task_id="check_value",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    sql=query_check_feature_distribution_duplicate(),
    pass_value=0,
    use_legacy_sql=False,
    location="asia-southeast2",
    )

    # [START feature_distribution]
    feature_distribution = DataflowCreatePythonJobOperator(
        task_id="run-feature-distribution-dataflow",
        trigger_rule=TriggerRule.ONE_SUCCESS,
        py_file=SCRIPT_PATH + "alert_ascore_feature.py",
        py_options=[],
        job_name='{{task.task_id}}',
        options={
            'project': PROJECT,
            "dataset_css": DATASET_CSS,
            "model_id": MODEL_ID,
            "calculation_type": CALCULATION_TYPE,
            "version": VERSION,
            "missing_value": MISSING_VALUE,
            "bucket_dataflow_location": BUCKET_DATAFLOW_LOCATION,
            "num_worker": NUM_WORKER,
            "max_worker": MAX_WORKER,
            "machine_type": MACHINE_TYPE,
            "df_subnet": DF_SUBNET,
        },
        location="asia-southeast2",
    )
    # [END feature_distribution]
    

    check_feature_distribution_exists >> [check_duplicate_value,pass_validate_feature_table] >> feature_distribution 
