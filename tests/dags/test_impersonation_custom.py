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

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# access custom python path
from contrib.utils import custom


DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
    'run_as_user': 'airflow_test_user'
}

dag = DAG(dag_id='impersonation_with_custom_pkg', default_args=args)


def print_today():
    print('Today is {}'.format(custom.today()))


PythonOperator(
    python_callable=print_today,
    task_id='exec_python_fn',
    dag=dag)
