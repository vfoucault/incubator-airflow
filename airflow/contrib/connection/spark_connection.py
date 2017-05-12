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
from airflow.contrib.connection.airflow_connection import AirflowConnection


class SparkConnection(AirflowConnection):
    """
    This connection is a extension of AirflowConnection that
    brings Spark Connection setting as attributes.
    :param conn_id: The connection id as configured in Airflow administration. When an
                    invalid connection_id is supplied, it raises AirflowException.
    :type conn_id: str
    """
    def __init__(self, conn_id='spark_default'):
        super(SparkConnection, self).__init__(conn_id=conn_id)
        if self.port:
            self.master = "{}:{}".format(self.host, self.port)
        else:
            self.master = self.host
        self.queue = self.extra_dejson.get('queue', None)
        self.deploy_mode = self.extra_dejson.get('deploy-mode', None)
        self.spark_home = self.extra_dejson.get('spark-home', None)
        self.spark_binary = self.extra_dejson.get('spark-binary', 'spark-submit')
