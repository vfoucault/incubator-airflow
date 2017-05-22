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
    template_fields = ('local_path', 'hdfs_path',)
    ui_color = WEB_COLORS['LIGHTYELLOW']

    @apply_defaults
    def __init__(self,
                 local_path = None,
                 hdfs_path = None,
                 conn_id = 'webhdfs_default',
                 proxy_user = None):
        super(HdfsOperator, self).__init__()
        self.local_path = local_path,
        self.hdfs_path = hdfs_path,
        self.conn_id = conn_id,
        self.hook = None
        self.proxy_user = None

    def execute(self, context):
        """
        """
        self.hook = WebHDFSHook(
            webhdfs_conn_id=self.conn_id,
            proxy_user=self.proxy_user
        )
        self._hook.submit(self._application)

    def on_kill(self):
        self._hook.on_kill()
