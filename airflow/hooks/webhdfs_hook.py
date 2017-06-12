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
import os

from airflow.hooks.base_hook import BaseHook
from airflow import configuration
import logging

from hdfs import InsecureClient, HdfsError

_kerberos_security_mode = configuration.get("core", "security") == "kerberos"
if _kerberos_security_mode:
    try:
        from hdfs.ext.kerberos import KerberosClient
    except ImportError:
        logging.error("Could not load the Kerberos extension for the WebHDFSHook.")
        raise
from airflow.exceptions import AirflowException


class AirflowWebHDFSHookException(AirflowException):
    pass


class WebHDFSHook(BaseHook):
    """
    Interact with HDFS. This class is a wrapper around the hdfscli library.
    """
    def __init__(self, webhdfs_conn_id='webhdfs_default', proxy_user=None):
        self.webhdfs_conn_id = webhdfs_conn_id
        self.proxy_user = proxy_user

    def get_conn(self):
        """
        Returns a hdfscli InsecureClient object.
        """
        nn_connections = self.get_connections(self.webhdfs_conn_id)
        for nn in nn_connections:
            try:
                logging.debug('Trying namenode {}'.format(nn.host))
                connection_str = 'http://{nn.host}:{nn.port}'.format(nn=nn)
                if _kerberos_security_mode:
                    client = KerberosClient(connection_str)
                else:
                    proxy_user = self.proxy_user or nn.login
                    client = InsecureClient(connection_str, user=proxy_user)
                client.status('/')
                logging.debug('Using namenode {} for hook'.format(nn.host))
                return client
            except HdfsError as e:
                logging.debug("Read operation on namenode {nn.host} failed with"
                              " error: {e.message}".format(**locals()))
        nn_hosts = [c.host for c in nn_connections]
        no_nn_error = "Read operations failed on the namenodes below:\n{}".format("\n".join(nn_hosts))
        raise AirflowWebHDFSHookException(no_nn_error)

    def check_for_path(self, hdfs_path):
        """
        Check for the existence of a path in HDFS by querying FileStatus.
        """
        c = self.get_conn()
        return bool(c.status(hdfs_path, strict=False))

    def load_file(self, source, destination, overwrite=True, parallelism=1,
                  **kwargs):
        """
        Uploads a file to HDFS

        :param source: Local path to file or folder. If a folder, all the files
          inside of it will be uploaded (note that this implies that folders empty
          of files will not be created remotely).
        :type source: str
        :param destination: PTarget HDFS path. If it already exists and is a
          directory, files will be uploaded inside.
        :type destination: str
        :param overwrite: Overwrite any existing file or directory.
        :type overwrite: bool
        :param parallelism: Number of threads to use for parallelization. A value of
          `0` (or negative) uses as many threads as there are files.
        :type parallelism: int
        :param \*\*kwargs: Keyword arguments forwarded to :meth:`upload`.


        """
        c = self.get_conn()
        c.upload(hdfs_path=destination,
                 local_path=source,
                 overwrite=overwrite,
                 n_threads=parallelism,
                 **kwargs)
        logging.debug("Uploaded file {} to {}".format(source, destination))

    def move(self, source, destination):
        """
        Move, rename a file/folder from source to destination
        
        :param source: HDFS path to file or folder. If ended with a wildcard (*), 
            the all the files and folders within this directory are moved to the
            dest_path
        :type source: str
        :param destination: target HDFS path. if source ends with a wildcard (*y) then 
            it must be a directory
        :type destination: str
        """
        c = self.get_conn()
        try:
            if os.path.basename(source) == "*":
                status = c.status(hdfs_path=os.path.dirname(source))
                if status['type'] == "DIRECTORY":
                    basedir = os.path.dirname(source)
                    contents = c.list(hdfs_path=basedir)
                    logging.debug("Moving {} file(s) from {} to {}".format(len(contents), basedir, destination))
                    map(lambda x: c.rename(hdfs_src_path="{}/{}".format(basedir, x), hdfs_dst_path="{}/{}".format(destination, x)), contents)
                else:
                    raise AirflowWebHDFSHookException("Destination is not a directory")
            else:
                c.status(hdfs_path=source)
                logging.debug("Moving {} to {}".format(source, destination))
                c.rename(hdfs_src_path=source, hdfs_dst_path=destination)
        except HdfsError as error:
            raise AirflowWebHDFSHookException("hdfs_path {} does not exists. Error : {}".format(source, error))

    def delete(self, hdfs_path, recursive=False):
        """
        Delete a hdfs path
        
        :param hdfs_path: HDFS path to file or folder to remove
        :type source_path: str
        :param recursive: Do a recursive removal, mandatroy for directories
        :type dest_path: Bool
        """
        c = self.get_conn()
        c.delete(hdfs_path, recursive=recursive)
        logging.debug("Deleted path {} with recursive {}".format(hdfs_path, recursive))

    def get(self, source, destination, overwrite=True, parallelism=1):
        """
        Get a hdfs file/folder to the local filesystem
        
        :param source: HDFS path to file or folder. 
        :type source_path: str
        :param destination: the local target path.
        :type dest_path: str
        :param overwrite: Wheter to overwrite local copy of file/directory if exists
        :type: Bool
        :param parallelism: Number of threads to use for parallelization. A value of
          `0` (or negative) uses as many threads as there are files.
        :type parallelism: int
        """
        c = self.get_conn()
        c.download(source, destination, overwrite=overwrite, n_threads=parallelism, **kwargs)
        logging.debug("Download path {} to localpath {}".format(source, source))

    def makedirs(self, hdfs_path, permission=755):
        """
        Create HDFS folders
        
        :param hdfs_path: HDFS path to folder. Recursive is implicit
        :type source_path: str
        :param permission: octal rights for new folder 
        :type dest_path: str
        
        """
        c = self.get_conn()
        c.makedirs(hdfs_path, permission)
        logging.debug("making directory {}".format(hdfs_path))



