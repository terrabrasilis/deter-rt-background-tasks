import sys

from airflow.operators.python import (
    PythonVirtualenvOperator,
    BranchPythonVirtualenvOperator,
    ShortCircuitOperator,
)
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.smtp.operators.smtp import EmailOperator


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

            deterRTDataChecker = HTTPDataChecker(log_level=log_level)

            if deterRTDataChecker.has_new_data():
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
            from tasks.sqlview_data_checker import SQLViewDataChecker

            http_collector = HTTPCollector(log_level=log_level)
            http_collector.read_data()

            sqlview_data_checker = SQLViewDataChecker(log_level=log_level)
            if sqlview_data_checker.has_new_data():
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
            from tasks.deter_rt_loader import DeterRTLoader

            loader = DeterRTLoader(log_level=log_level)
            loader.data_loader()

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
            from tasks.deter_rt_transformer import DeterRTTransformer

            transformer = DeterRTTransformer(log_level=log_level)
            transformer.process_data()

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
            from tasks.deter_rt_validator import DeterRTValidator
            validator = DeterRTValidator(log_level=log_level)
            validator.validation()

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


    def report_task_operator(self, updated_date, email_to: list):
        sys.path.append(self.project_dir)
        from utils.template_loader import TemplateLoader
        
        html_content = TemplateLoader.read_html_template(project_dir=self.project_dir,
            file_name='report_by_email.html',
            task_name=f'DETER-RT background task processing',
            date=updated_date
        )

        def fnc_operator(project_dir: str, updated_date:str, log_level: str):
            sys.path.append(project_dir)
            from tasks.output_database import OutputDatabase

            outdb = OutputDatabase(log_level=log_level)
            report = outdb.get_info_to_report()
            report = f"""
            <p>Automatic validation report - DETER-RT</p>
            <ul>
                <li>Total alerts sent for audit: <b>{report['alerts_to_audit']}</b></li>
                <li>Total alerts approved by automatic audit: <b>{report['alerts_approved']}</b></li>
            </ul>
            """
            return report
            

        report_task = PythonVirtualenvOperator(
            task_id="report_task",
            requirements=self.requirements,
            venv_cache_path=f"{self.venv_path}",
            python_callable=fnc_operator,
            provide_context=True,
            op_args=[f"{self.project_dir}", f"{updated_date}", f"{self.log_level}"],
            retries=0,
        )

        # conn_id: The Airflow connection ID to use for sending the email (e.g., an SMTP connection).
        report_task_email = EmailOperator(
            task_id='report_task_email',
            conn_id='SMTP_INPE',
            mime_charset="utf-8",
            to=email_to,
            subject=f'✅ DETER-RT - Data released for audit in {updated_date}',
            html_content=html_content,
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        report_task.set_downstream(report_task_email)
        return report_task
        