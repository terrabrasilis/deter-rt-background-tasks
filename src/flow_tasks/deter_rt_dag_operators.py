import sys

from airflow.operators.python import (
    PythonVirtualenvOperator,
    BranchPythonVirtualenvOperator,
    ShortCircuitOperator,
)
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable



class BaseDagOperators:

    def __init__(self, venv_path, project_dir, log_level=None):
        self.project_dir = project_dir
        self.venv_path = venv_path
        self.log_level = log_level
        self.requirements = [
            "requests",
            "psycopg2-binary",
            "geopandas==0.13.2",
            "fiona==1.9.6",
            "geoalchemy2",
            "webdavclient3",
        ]

    def check_configuration_environment_operator(self):

        def fnc_operator():

            deter_amazonia_db_url = BaseHook.get_connection("DETER_RT_DETER_AMAZONIA_DB_URL")
            deter_rt_db_url = BaseHook.get_connection("DETER_RT_DB_URL")
            deter_rt_http_url = BaseHook.get_connection("DETER_RT_WEBDAV_NEXTCLOUD")

            if not deter_amazonia_db_url or not deter_amazonia_db_url.get_uri():
                raise Exception(
                    f"Missing DETER_RT_DETER_AMAZONIA_DB_URL airflow conection configuration."
                )

            if not deter_rt_db_url or not deter_rt_db_url.get_uri():
                raise Exception(
                    "Missing DETER_RT_DB_URL airflow conection configuration."
                )
            
            if not deter_rt_http_url or not deter_rt_http_url.get_uri():
                raise Exception(
                    "Missing DETER_RT_WEBDAV_NEXTCLOUD airflow HTTP configuration."
                )

            return True

        return ShortCircuitOperator(
            task_id="check_configuration",
            provide_context=True,
            python_callable=fnc_operator,
        )

    def check_data_availability_task_operator(self):

        def fnc_operator(project_dir: str, log_level: str):
            """should return one task_id: collector_task or log_registry_task"""
            sys.path.append(project_dir)
            from tasks.http_data_checker import HTTPDataChecker
            from tasks.sqlview_data_checker import SQLViewDataChecker

            deterRTDataChecker = HTTPDataChecker(log_level=log_level)
            #opticalDeterDataChecker = SQLViewDataChecker(log_level=log_level)

            if deterRTDataChecker.has_new_data(): # and opticalDeterDataChecker.has_new_data():
                return "collector_task"
            else:
                return "log_registry_task"

        return BranchPythonVirtualenvOperator(
            task_id="check_data_availability",
            requirements=self.requirements,
            venv_cache_path=f"{self.venv_path}",
            python_callable=fnc_operator,
            provide_context=True,
            do_xcom_push=True,
            op_kwargs={
                "project_dir": f"{self.project_dir}",
                "log_level": f"{self.log_level}",
            },
            email_on_retry=True,
            retries=3,
        )

    def collector_task_operator(self):

        def fnc_operator(project_dir: str, log_level: str):
            sys.path.append(project_dir)
            from tasks.http_collector import HTTPCollector
            from tasks.sqlview_collector import SQLViewCollector

            http_collector = HTTPCollector(log_level=log_level)
            http_collector.read_data()

            sqlview_collector = SQLViewCollector(log_level=log_level)
            sqlview_collector.read_data()

        return PythonVirtualenvOperator(
            task_id="collector_task",
            requirements=self.requirements,
            venv_cache_path=f"{self.venv_path}",
            python_callable=fnc_operator,
            provide_context=True,
            op_args=[f"{self.project_dir}", f"{self.log_level}"],
            email_on_retry=True,
            retries=3,
        )

    def loader_task_operator(self):

        def fnc_operator(project_dir: str, log_level: str):
            sys.path.append(project_dir)
            pass

        return PythonVirtualenvOperator(
            task_id="loader_task",
            requirements=self.requirements,
            venv_cache_path=f"{self.venv_path}",
            python_callable=fnc_operator,
            provide_context=True,
            op_args=[f"{self.project_dir}", f"{self.log_level}"],
            email_on_retry=True,
            retries=3,
        )

    def transformer_task_operator(self):

        def fnc_operator(project_dir: str, log_level: str):
            sys.path.append(project_dir)
            pass

        return PythonVirtualenvOperator(
            task_id="transformer_task",
            requirements=self.requirements,
            venv_cache_path=f"{self.venv_path}",
            python_callable=fnc_operator,
            provide_context=True,
            op_args=[f"{self.project_dir}", f"{self.log_level}"],
        )

    def validator_task_operator(self):

        def fnc_operator(project_dir: str, log_level: str):
            sys.path.append(project_dir)
            pass

        return PythonVirtualenvOperator(
            task_id="validator_task",
            requirements=self.requirements,
            venv_cache_path=f"{self.venv_path}",
            python_callable=fnc_operator,
            provide_context=True,
            op_args=[f"{self.project_dir}", f"{self.log_level}"],
        )

    def log_registry_task_operator(self):
        """Write log into collector_log table"""

        def fnc_operator(
            project_dir: str, log_level: str, branch_target: str
        ):

            sys.path.append(project_dir)
            from tasks.log_registry import LogRegistry

            reg = LogRegistry(log_level=log_level)

            if branch_target == "collector_task":
                # then the whole flow is ok.
                description = f"DETER RT data has been successfully processed.."
            else:
                # If we don't have new data.
                description = f"There is no new data."

            reg.write(description=description, success=True)

        return PythonVirtualenvOperator(
            task_id="log_registry_task",
            requirements=self.requirements,
            venv_cache_path=f"{self.venv_path}",
            python_callable=fnc_operator,
            provide_context=True,
            trigger_rule=TriggerRule.NONE_FAILED,
            op_kwargs={
                "project_dir": f"{self.project_dir}",
                "log_level": f"{self.log_level}",
                "branch_target": "{{ ti.xcom_pull(task_ids='check_data_availability') }}",
            },
        )

    def email_operator(self, updated_date, email_to: list):
        """Does working only with configurations on stack or in airflow.cfg"""

        return EmailOperator(
            task_id="send_email_task",
            mime_charset="utf-8",
            to=email_to,
            subject="Airflow - {{ dag_run.dag_id }}",
            html_content=f"""<h3>Aviso de execução</h3><p>A tarefa executou com <b>sucesso</b> em {updated_date}.</p>""",
        )
