import pyarrow as pa
from airflow.hooks.base_hook import BaseHook
import os
import subprocess
from pyarrow import fs

DEFAULT_NUMBER_REPLICATION = 1

class PyarrowHdfsHook(BaseHook):
    def __init__(self, hdfs_conn_id):
        self.hdfs_conn_id = hdfs_conn_id
        self.conn = None

    def get_replication(self, extra_conf):
        if extra_conf is not None and isinstance(extra_conf, dict) and 'dfs.replication' in extra_conf:
            num_replication = int(extra_conf.get('dfs.replication'))
        else:
            num_replication = DEFAULT_NUMBER_REPLICATION

        return num_replication

    def get_conn(self):
        """
        Kết nối đến HDFS và trả về một HadoopFileSystem object
        """
        if self.conn is not None:
            return self.conn

        conn = self.get_connection(self.hdfs_conn_id)
        host = conn.host
        user = conn.login
        port = conn.port
        extra_conf = conn.extra_dejson
        num_replication = self.get_replication(extra_conf=extra_conf)
        os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
        os.environ['HADOOP_HOME'] = '/home/hoang/workarea/software/hadoop-3.3.6'
        result = subprocess.Popen(['hdfs', 'classpath', '--glob'],
                                  stdout=subprocess.PIPE).communicate()[0]
        os.environ['CLASSPATH'] = str(result)
        self.conn = fs.HadoopFileSystem(host, port, user=user, replication=num_replication, extra_conf=extra_conf)
        return self.conn
