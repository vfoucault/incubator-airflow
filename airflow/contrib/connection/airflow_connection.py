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
from airflow.hooks.base_hook import BaseHook


class AirflowConnection(object):
    """
    Abstract base class for AirflowConnection, 
    which are meant as an interface to interact with connection.
    :param conn_id: The connection id as configured in Airflow administration. When an
                    invalid connection_id is supplied, it raises AirflowException
    :type conn_id: str
    """
    def __init__(self, conn_id):

        self.conn_id = conn_id
        self.hook = BaseHook

        conn_data = self.hook.get_connection(conn_id)

        self.host = conn_data.host
        self.port = conn_data.port
        self.conn_type = conn_data.conn_type
        self.schema = conn_data.schema
        self.login = conn_data.login
        self.password = conn_data.password
        self.extra = conn_data.extra
        self.extra_dejson = conn_data.extra_dejson
