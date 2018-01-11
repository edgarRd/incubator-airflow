# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash_operator import BashOperator
from airflow.executors import SequentialExecutor


from tests.test_utils.fake_datetime import FakeDatetime

DEFAULT_DATE = datetime(2016, 1, 1)

default_args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
    'run_as_user': 'airflow_test_user'
}

dag = DAG(dag_id='impersonation_subdag', default_args=default_args)


def print_today():
    dt = FakeDatetime.utcnow()
    print('Today is {}'.format(dt.strftime('%Y-%m-%d')))


subdag = DAG('impersonation_subdag.test_subdag_operation',
             default_args=default_args)


class CheckConfigSubDagOperator(SubDagOperator):

    """Sub Class of SubDagOperator to assert that the config path has correctly been
    propagated, which gets created on pre_execute hook."""

    def execute(self, context):
        super(CheckConfigSubDagOperator, self).execute(context)
        assert self.used_tmp_cfg_path and len(self.used_tmp_cfg_path) > 0, \
            "cfg_path must be set!"


subdag_operator = CheckConfigSubDagOperator(task_id='test_subdag_operation',
                                            subdag=subdag,
                                            executor=SequentialExecutor(),
                                            dag=dag)

PythonOperator(
    python_callable=print_today,
    task_id='exec_python_fn',
    dag=subdag)

BashOperator(
    task_id='exec_bash_operator',
    bash_command='echo "Running within SubDag"',
    dag=subdag
)
