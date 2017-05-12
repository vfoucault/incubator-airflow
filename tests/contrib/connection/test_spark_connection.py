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

from airflow.contrib.connection.spark_connection import SparkConnection
from airflow.exceptions import AirflowException
from airflow import configuration, models
from airflow.utils import db


class TestSparkConnection(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='custom_test_spark_1', conn_type='spark',
                host='yarn', port=1234,
                extra='{"queue": "root.prod", "deploy-mode": "cluster", "spark-binary": "custom-spark-submit", "spark-home": "/path/to/spark_home"}',
                login='my_user', password='veryS3cret')
        )
        db.merge_conn(
            models.Connection(
                conn_id='custom_test_spark_2', conn_type='spark',
                host='yarn', extra='{"queue": "root.prod", "spark-binary": "custom-spark-submit"}')
        )

    def test_init_with_spark_default(self):
        # Given
        conn_id = 'spark_default'

        # When
        conn = SparkConnection(conn_id=conn_id)

        # Then
        self.assertEquals(conn.conn_type, 'spark')
        self.assertEquals(conn.host, 'yarn')
        self.assertEquals(conn.port, None)
        self.assertEquals(conn.login, None)
        self.assertEquals(conn.password, None)
        self.assertEquals(conn.extra, u'{"queue": "root.default"}')
        self.assertEquals(conn.extra_dejson, {'queue': 'root.default'})
        # spark specific attributes
        self.assertEquals(conn.master, 'yarn')
        self.assertEquals(conn.queue, 'root.default')
        self.assertEquals(conn.deploy_mode, None)
        self.assertEquals(conn.spark_home, None)
        self.assertEquals(conn.spark_binary, 'spark-submit')

    def test_init_with_default(self):
        # Given / When
        conn = SparkConnection()

        # Then
        self.assertEquals(conn.master, 'yarn')
        self.assertEquals(conn.queue, 'root.default')
        self.assertEquals(conn.deploy_mode, None)
        self.assertEquals(conn.spark_home, None)
        self.assertEquals(conn.spark_binary, 'spark-submit')

    def test_init_with_custom_test_spark_1(self):
        # Given
        conn_id = 'custom_test_spark_1'

        # When
        conn = SparkConnection(conn_id=conn_id)

        # Then
        self.assertEquals(conn.master, 'yarn:1234')
        self.assertEquals(conn.queue, 'root.prod')
        self.assertEquals(conn.deploy_mode, 'cluster')
        self.assertEquals(conn.spark_home, '/path/to/spark_home')
        self.assertEquals(conn.spark_binary, 'custom-spark-submit')

    def test_init_with_custom_test_spark_2(self):
        # Given
        conn_id = 'custom_test_spark_2'

        # When
        conn = SparkConnection(conn_id=conn_id)

        # Then
        self.assertEquals(conn.master, 'yarn')
        self.assertEquals(conn.queue, 'root.prod')
        self.assertEquals(conn.deploy_mode, None)
        self.assertEquals(conn.spark_home, None)
        self.assertEquals(conn.spark_binary, 'custom-spark-submit')

    def test_init_with_not_existing_should_raise(self):
        # Given
        conn_id = 'my_random_and_not_existing_connection'

        # When / Then
        with self.assertRaises(AirflowException):
            SparkConnection(conn_id=conn_id)

if __name__ == '__main__':
    unittest.main()
