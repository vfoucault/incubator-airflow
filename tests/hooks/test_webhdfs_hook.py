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
import os
import unittest
import tempfile

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
        self.assertTrue(hook.check_for_path('/'))

    def test_upload_file(self):
        # Given
        hook = WebHDFSHook('webhdfs_test')
        tmp_fname = tempfile.mkstemp(dir='/tmp')[1]
        # When

        with open(tmp_fname, 'wb') as tmpfile:
            tmpfile.seek(1 * 1024 * 1024 - 1)
            tmpfile.write(b'0')
            tmpfile.seek(0)
        hook.load_file(tmp_fname, '/tmp')

        # Then
        nfile_status = hook.get_conn().status(tmp_fname)
        self.assertEquals(nfile_status['blockSize'], 134217728)
        os.unlink(tmp_fname)


    def test_make_dir(self):
        # Given
        hook = WebHDFSHook('webhdfs_test')
        tmp_dirnames = [next(tempfile._get_candidate_names()), next(tempfile._get_candidate_names())]

        # When
        hook.makedirs('/tmp/{}/{}'.format(tmp_dirnames[0], tmp_dirnames[1]))

        # Then

        tmp1_status = hook.get_conn().list('/tmp/')
        tmp2_status = hook.get_conn().list('/tmp/{}'.format(tmp_dirnames[0]))
        self.assertIn(tmp_dirnames[0], tmp1_status)
        self.assertIn(tmp_dirnames[1], tmp2_status)

if __name__ == '__main__':
    unittest.main()
