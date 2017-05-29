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
from hdfs import InsecureClient, HdfsError

from airflow.utils import db

from airflow.hooks.webhdfs_hook import WebHDFSHook, AirflowWebHDFSHookException


class TestWebHdfsHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='webhdfs_test', conn_type='hdfs',
                host='localhost', port="51351")
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
        tmp_filename = os.path.basename(tmp_fname)

        # Then
        nfile_status = hook.get_conn().status("/tmp/{}".format(tmp_filename))
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

    def test_move_file(self):
        # Given
        hook = WebHDFSHook('webhdfs_test')
        source_file = tempfile.mkstemp(dir='/tmp')[1]

        with open(source_file, 'wb') as tmpfile:
            tmpfile.seek(1 * 1024 * 1024 - 1)
            tmpfile.write(b'0')
            tmpfile.seek(0)
        hook.load_file(source_file, '/tmp')
        source_filename = os.path.basename(source_file)
        dest_filename = next(tempfile._get_candidate_names())

        # When
        hook.move("/tmp/{}".format(source_filename), "/tmp/{}".format(dest_filename))

        # Then
        with self.assertRaises(HdfsError):
            hook.get_conn().status("/tmp/{}".format(source_filename))

        dest_status = hook.get_conn().status("/tmp/{}".format(dest_filename))
        self.assertEquals(dest_status['blockSize'], 134217728)
        os.unlink(source_file)

    def test_move_file_for_non_existing_file(self):
        # Given
        hook = WebHDFSHook('webhdfs_test')
        tmp_filename = next(tempfile._get_candidate_names())
        tmp_destname = next(tempfile._get_candidate_names())

        # When / Then
        with self.assertRaises(AirflowWebHDFSHookException):
            hook.move("/tmp/{}".format(tmp_filename), "/tmp/{}".format(tmp_destname))

    def test_move_file_to_existing_file(self):
        # Given
        hook = WebHDFSHook('webhdfs_test')
        source_file = tempfile.mkstemp(dir='/tmp')[1]
        dest_file = tempfile.mkstemp(dir='/tmp')[1]
        source_filename = os.path.basename(source_file)
        dest_filename = os.path.basename(dest_file)
        hook.load_file(source_file, "/tmp")
        hook.load_file(dest_file, "/tmp")

        # When / Then
        with self.assertRaises(AirflowWebHDFSHookException):
            hook.move("/tmp/{}".format(source_filename), "/tmp/{}".format(dest_filename))

    def test_move_all_files_for_directory(self):
        # Given
        hook = WebHDFSHook('webhdfs_test')
        tmpdir1 = next(tempfile._get_candidate_names())
        tmpdir2 = next(tempfile._get_candidate_names())

        source_files = [tempfile.mkstemp(dir='/tmp')[1] for x in range(1, 10)]
        hook.get_conn().makedirs("/tmp/{}".format(tmpdir1))
        hook.get_conn().makedirs("/tmp/{}".format(tmpdir2))
        map(lambda x: hook.load_file(x, "/tmp/{}".format(tmpdir1)), source_files)

        # When
        hook.move("/tmp/{}".format(tmpdir1), "/tmp/{}".format(tmpdir2))

        # Then
        self.assertEquals(len(source_files), len(hook.get_conn().list("/tmp/{}".format(tmpdir2))))
        self.assertListEqual([], hook.get_conn().list("/tmp/{}".format(tmpdir1)))

        map(lambda x: os.unlink(x), source_files)

    def test_move_all_files_for_non_existing_directory(self):
        # Given
        hook = WebHDFSHook('webhdfs_test')
        tmpdir1 = next(tempfile._get_candidate_names())
        tmpdir2 = next(tempfile._get_candidate_names())

        # When / Then
        with self.assertRaises(AirflowWebHDFSHookException):
            hook.move("/tmp/{}".format(tmpdir1), "/tmp/{}".format(tmpdir2))

if __name__ == '__main__':
    unittest.main()
