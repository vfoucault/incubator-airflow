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
import logging

from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.settings import WEB_COLORS

log = logging.getLogger(__name__)


class HdfsOperator(BaseOperator):
    """
    
    """
    ui_color = WEB_COLORS['LIGHTYELLOW']

    @apply_defaults
    def __init__(self, conn_id = 'webhdfs_default', proxy_user = None, *args, **kwargs):
        super(HdfsOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id,
        self.hook = None
        self.proxy_user = None

class HdfsMoveOperator(HdfsOperator):
    """
    
    """
    template_fields = ('source_path', 'target_path')

    @apply_defaults
    def __init__(self, source_path, target_path, *args, **kwargs):
        super(HdfsMoveOperator, self).__init__(*args, **kwargs)
        self.source_path = source_path
        self.target_path = target_path

    def execute(self, context):
        """
        """
        self.hook = WebHDFSHook(
            webhdfs_conn_id=self.conn_id,
            proxy_user=self.proxy_user
        )
        self.hook.move(source_path=self.source_path, dest_path=self.target_path)


class HdfsCreateDirectoryOperator(HdfsOperator):
    """
    
    """
    template_fields = ('hdfs_path',)

    @apply_defaults
    def __init__(self, hdfs_path, permission=755, *args, **kwargs):
        super(HdfsCreateDirectoryOperator, self).__init__(*args, **kwargs)
        self.hdfs_path = hdfs_path
        self.permission = permission

    def execute(self, context):
        """
        """
        self.hook = WebHDFSHook(
            webhdfs_conn_id=self.conn_id,
            proxy_user=self.proxy_user
        )
        self.hook.makedirs(hdfs_path=self.hdfs_path, permission=self.permission)


class HdfsDeleteOperator(HdfsOperator):
    """
    
    """
    template_fields = ('hdfs_path',)

    @apply_defaults
    def __init__(self, hdfs_path, recursive=False, *args, **kwargs):
        super(HdfsCreateDirectoryOperator, self).__init__(*args, **kwargs)
        self.hdfs_path = hdfs_path
        self.recursive = recursive

    def execute(self, context):
        """
        """
        self.hook = WebHDFSHook(
            webhdfs_conn_id=self.conn_id,
            proxy_user=self.proxy_user
        )
        self.hook.delete(hdfs_path=self.hdfs_path, recursive=self.recursive)


class HdfsGetFileOperator(HdfsOperator):
    """
    
    """
    template_fields = ('hdfs_path', 'local_path')

    @apply_defaults
    def __init__(self, hdfs_path, local_path, overwrite=True, *args, **kwargs):
        super(HdfsGetFileOperator, self).__init__(*args, **kwargs)
        self.hdfs_path = hdfs_path
        self.local_path = local_path
        self.overwrite = overwrite

    def execute(self, context):
        """
        """
        self.hook = WebHDFSHook(
            webhdfs_conn_id=self.conn_id,
            proxy_user=self.proxy_user
        )
        self.hook.get(hdfs_path=self.hdfs_path, local_path=self.local_path, overwrite=self.overwrite)


class HdfsPutFileOperator(HdfsOperator):
    """
    
    """
    template_fields = ('hdfs_path', 'local_path')

    @apply_defaults
    def __init__(self, hdfs_path, local_path, overwrite=False, parallelism=1, *args, **kwargs):
        super(HdfsPutFileOperator, self).__init__(*args, **kwargs)
        self.hdfs_path = hdfs_path
        self.local_path = local_path
        self.overwrite = overwrite
        self.parallelism = parallelism

    def execute(self, context):
        """
        """
        self.hook = WebHDFSHook(
            webhdfs_conn_id=self.conn_id,
            proxy_user=self.proxy_user
        )
        self.hook.load_file(source=local_path,
                            destination=self.hdfs_path,
                            overwrite=self.overwrite,
                            parallelism=self.parallelism)
