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
from airflow.exceptions import AirflowException

from airflow.contrib.connection.airflow_connection import AirflowConnection
from airflow import configuration, models
from airflow.utils import db

class TestAirflowConnection(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestAirflowConnection, self).__init__(*args, **kwargs)
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='custom_test_one', conn_type='custom',
                host='somehost', port=1234, extra='{"key1": "value1"}',
                login='my_user', password='veryS3cret')
        )

    def test_init_with_airflow_db(self):
        # Given
        conn_id = 'airflow_db'

        # When
        conn = AirflowConnection(conn_id=conn_id)

        # Then
        self.assertEquals(conn.conn_type, 'mysql')
        self.assertEquals(conn.host, 'localhost')
        self.assertEquals(conn.port, None)
        self.assertEquals(conn.login, 'root')
        self.assertEquals(conn.password, None)
        self.assertEquals(conn.extra, None)
        self.assertEquals(conn.extra_dejson, {})

    def test_init_with_custom_test_one(self):
        # Given
        conn_id = 'custom_test_one'

        # When
        conn = AirflowConnection(conn_id=conn_id)

        # Then
        self.assertEquals(conn.conn_type, 'custom')
        self.assertEquals(conn.host, 'somehost')
        self.assertEquals(conn.port, 1234)
        self.assertEquals(conn.login, 'my_user')
        self.assertEquals(conn.password, 'veryS3cret')
        self.assertEquals(conn.extra, u'{"key1": "value1"}')
        self.assertEquals(conn.extra_dejson, {"key1": "value1"})

    def test_init_with_not_existing_should_raise(self):
        # Given
        conn_id = 'my_random_and_not_existing_connection'

        # When / Then
        with self.assertRaises(AirflowException):
            AirflowConnection(conn_id=conn_id)

if __name__ == '__main__':
    unittest.main()
