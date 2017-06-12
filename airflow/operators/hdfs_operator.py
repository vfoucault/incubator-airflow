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
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)



class AirflowHdfsOperatorException(AirflowException):
    pass


class HdfsOperator(BaseOperator):
    """
    
    """

    template_fields = ('source', 'destination', 'hdfs_path', )

    ui_color = WEB_COLORS['LIGHTYELLOW']

    @apply_defaults
    def __init__(self,
                 optype=None,
                 source=None,
                 destination=None,
                 hdfs_path=None,
                 recursive=False,
                 overwrite=False,
                 parallelism=1,
                 permission=755,
                 conn_id='webhdfs_default',
                 proxy_user=None,
                 *args,
                 **kwargs):
        super(HdfsOperator, self).__init__(*args, **kwargs)
        self.optype = optype
        self.source = source
        self.destination = destination
        self.hdfs_path = hdfs_path
        self.recursive = recursive
        self.overwrite = overwrite
        self.parallelism = parallelism
        self.permission = permission
        self.conn_id = conn_id
        self.hook = None
        self.proxy_user = proxy_user

    def pre_execute(self, context):
        """
        
        :param context: 
        :return: 
        """
        self.hook = WebHDFSHook(
            webhdfs_conn_id=self.conn_id,
            proxy_user=self.proxy_user
        )

    def test_variables(self, variable_list):
        """
        Test wether some variables are set to None
        :param variable_list: Variables names to test
        :type variable_list: List of Strings
        :return: Boolean : True if none of the variables are set to None
        """
        result = [{'name': x, 'value': getattr(self, x)} for x in variable_list if not getattr(self, x) ]
        if len(result) > 0:
            logging.error('Some variables aren\'t defined')
            map(lambda x: logging.error('{} attribute is empty'.format(x['name'])))
            raise AirflowHdfsOperatorException("Some required attribute are empty !")
        else:
            return True

    def execute(self, context):
        """
        
        :param context: 
        :return: 
        """
        self.test_variables(['optype'])
        if self.optype == 'move':
            if self.test_variables(['source', 'destination']):
                self.hook.move(source=self.source, destination=self.destination)
        elif self.optype == 'makedirs':
            if self.test_variables(['hdfs_path']):
                self.hook.makedirs(hdfs_path=self.hdfs_path, permission=self.permission)
        elif self.optype == 'delete':
            if self.test_variables(['hdfs_path']):
                self.hook.delete(hdfs_path=self.hdfs_path, recursive=self.recursive)
        elif self.optype == 'download':
            if self.test_variables(['source', 'destination']):
                self.hook.get(source=self.source, destination=self.destination, overwrite=self.overwrite)
        elif self.optype == 'upload':
            if self.test_variables(['source', 'destination']):
                self.hook.load_file(source=self.source,
                                    destination=self.destination,
                                    overwrite=self.overwrite,
                                    parallelism=self.parallelism)




