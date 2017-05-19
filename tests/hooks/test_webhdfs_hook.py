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
from airflow import configuration, models
from hdfs import InsecureClient

from airflow.utils import db

from airflow.hooks.webhdfs_hook import WebHDFSHook


class TestWebHdfsHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='webhdfs_test', conn_type='hdfs',
                host='localhost', port="61011")
        )

    def test_get_connection(self):
        # Given
        hook = WebHDFSHook('webhdfs_test')

        # When
        client = hook.get_conn()

        # Then
        self.assertEquals(InsecureClient, type(client))
        self.assertTrue(hook.check_for_path())

    def test_upload_file(self):
        # Given
        hook = WebHDFSHook('webhdfs_test')
        client = hook.get_conn()

        # When
        client.upload('/tmp','/etc/hosts')

        # Then
        nfile_status = hook.get_conn().list('/tmp', status=True)
        self.assertTrue(hook.get_conn().list('/tmp', status=True), type(client))

if __name__ == '__main__':
    unittest.main()
