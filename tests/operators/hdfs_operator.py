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
#

import unittest
import datetime


from airflow import DAG, configuration
from airflow.hooks.webhdfs_hook import WebHDFSHook

from airflow.operators.hdfs_operator import HdfsOperator, AirflowHdfsOperatorException

DEFAULT_DATE = datetime.datetime(2017, 1, 1)


class TestHdfsOperator(unittest.TestCase):

    def setUp(self):
        print "running test %s" % self._testMethodName
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_setup_with_move_operation(self):
        # Given
        conn_id = 'webhdfs_test'

        # When
        operator = HdfsOperator(
            task_id='a_move_operation',
            optype='move',
            source='/some/hdfs/directory',
            destination='/some/other/path',
            conn_id=conn_id,
            dag=self.dag
        )

        operator.pre_execute("")

        # Then
        self.assertIsInstance(operator.hook, WebHDFSHook)


    def test_setup_with_move_operation_with_unset_variables(self):
        # Given
        conn_id = 'webhdfs_test'

        # When
        operator = HdfsOperator(
            task_id='a_move_operation',
            optype='move',
            destination='/some/other/path',
            conn_id=conn_id,
            dag=self.dag,
        )

        with self.assertRaises(AirflowHdfsOperatorException):
            operator.execute()


if __name__ == '__main__':
    unittest.main()
