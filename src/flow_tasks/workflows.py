""" An ETL to process DETER-RT input data. """

from datetime import datetime
import pathlib
import sys
import pendulum
from airflow import DAG
from airflow.models import Variable

# used to load the location where base dag is
dag_dir = str(pathlib.Path(__file__).parent.resolve().absolute())
sys.path.append(dag_dir)

# used to load the location where code of tasks are
project_dir = str(pathlib.Path(__file__).parent.parent.resolve().absolute())
sys.path.append(project_dir)

from deter_rt_dag_operators import BaseDagOperators

# get a list of emails to send to, as a string in this format: email1@host.com,email2@host.com,email3@host.com
EMAIL_TO = Variable.get("GENERAL_EMAIL_TO")
# define a log level (CRITICAL = 50, FATAL = CRITICAL, ERROR = 40, WARNING = 30, WARN = WARNING, INFO = 20, DEBUG = 10, NOTSET = 0)
LOG_LEVEL = Variable.get("DETER_RT_LOG_LEVEL")

if not EMAIL_TO or not LOG_LEVEL:
    raise Exception(
        f"Missing GENERAL_EMAIL_TO and DETER_RT_LOG_LEVEL variables into airflow configuration."
    )


EMAIL_TO = str(EMAIL_TO).split(",")
# Default arguments for all tasks. Precedence is the value at task instantiation.
task_default_args = {
    "start_date": pendulum.datetime(year=2025, month=1, day=15, tz="America/Sao_Paulo"),
    "owner": "airflow",
    "depends_on_past": False,
    "email": EMAIL_TO,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
# The path to the virtual environment location for this set of DAGs
venv_path = f"/opt/airflow/venv/inpe/deter_rt"

DAG_KEY = f"deter_rt_validation"
description = f"This flow represents the process to get files from NextCloud COIDS, import into PostGIS database, perform automatic validation and prepare for manual validation."

with DAG(
    dag_id=DAG_KEY,
    description=description,
    schedule=None,
    catchup=False,
    default_args=task_default_args,
) as process_dag:

    baseDag = BaseDagOperators(
        venv_path=venv_path, project_dir=project_dir, log_level=LOG_LEVEL
    )

    check_conf = baseDag.check_configuration_environment_operator()
    check_new_data = baseDag.check_data_availability_task_operator()
    collector = baseDag.collector_task_operator()
    loader = baseDag.loader_task_operator()
    transformer = baseDag.transformer_task_operator()
    validator = baseDag.validator_task_operator()
    log = baseDag.log_registry_task_operator()
    email_operator = baseDag.report_task_operator(
        datetime.today().strftime("%d/%m/%Y"), email_to=EMAIL_TO
    )
    process_failed = baseDag.failure_task_operator(
        datetime.today().strftime("%d/%m/%Y"), email_to=EMAIL_TO
    )

    check_conf >> check_new_data >> [collector, log]
    (
        collector
        >> loader
        >> transformer
        >> validator
        >> log
        >> [email_operator, process_failed]
    )
    log >> [email_operator, process_failed]
