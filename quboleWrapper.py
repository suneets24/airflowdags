from qds_sdk.qubole import Qubole
from qds_sdk.commands import HiveCommand, PrestoCommand, SparkCommand, Command
import io
import os
import time
from datetime import date, timedelta
from jinja2 import Template
from airflow.exceptions import AirflowException
import pymysql
import psycopg2
import boto3


class DataWarehouseDates(object):
    def __init__(self):
        self._seed_date = date(2000, 1, 2)
        self._seed_date_id = 2

    def date_add(self, date_str, days):
        date_parts = date_str.split('-')

        dt = date(int(date_parts[0]),
                  int(date_parts[1]),
                  int(date_parts[2]))

        new_dt = dt + timedelta(days=days)

        return new_dt.strftime('%Y-%m-%d')

    def date_id_from_date(self, year, month, day):
        delta = date(year, month, day) - self._seed_date

        return delta.days + self._seed_date_id

    def date_id_from_date_str(self, date_str):
        date_parts = date_str.split('-')

        return self.date_id_from_date(int(date_parts[0]),
                                      int(date_parts[1]),
                                      int(date_parts[2]))

    def ds_today(self, date_str):
        return self.date_add(date_str, 1)


class QuboleWrapper(object):
    def __init__(self, db_type, raw_sql, expected_runtime, dag_id, task_id, ds):
        Qubole.configure(api_token='%s' % os.environ['QUBOLE_API_TOKEN'])
        if db_type.upper() == 'PRESTO_CSV':
            self.label = 'presto_no_compression'
        elif db_type.upper() == 'PROD_PRESTO':
            self.label = 'Prod-Presto'
        elif db_type.upper() == 'DEV_PRESTO':
            self.label = 'Dev-Presto'
        elif db_type.upper() == 'HIVE':
            self.label = 'default'
        else:
            msg = 'Need to specify correct query type: presto_csv or presto_orc'
            raise Exception(msg)
        self.raw_sql = raw_sql
        self.db_type = db_type
        if expected_runtime == 0:
            self.expected_runtime = 7200  # 2 hour default
        else:
            self.expected_runtime = expected_runtime
        self.dag_id = dag_id
        self.task_id = task_id
        self.ds = ds

    def run_sql(self):
        command = None
        sql_stmt = None
        try:
            template = Template(self.raw_sql)
            dwd = DataWarehouseDates()
            sql_stmt = template.render(DS=self.ds,
                                       DS_TODAY=dwd.ds_today(self.ds),
                                       DS_DATE_ID=dwd.date_id_from_date_str(self.ds),
                                       DS_DATE_ADD=lambda days: dwd.date_add(self.ds, days),
                                       DS_TODAY_DATE_ADD=lambda days: dwd.date_add(dwd.ds_today(self.ds), days)
                                       )
            sql_stmt_with_tracking = '-- %s %s %s\n%s' % (self.dag_id, self.task_id, self.ds, sql_stmt)
            qubole_name = '%s_%s_%s' % (self.dag_id, self.task_id, self.ds)
            if 'PRESTO' in self.db_type.upper():
                command = PrestoCommand.create(query=sql_stmt_with_tracking, label=self.label, name=qubole_name)
            elif 'HIVE' in self.db_type.upper():
                command = HiveCommand.run(query=sql_stmt_with_tracking, label=self.label, name=qubole_name)
            else:
                raise AirflowException('Invalid db_type specified.')

            self.monitor_command(command, sql_stmt_with_tracking)
        except Exception as e:
            if command is None:
                raise AirflowException('run_sql call for %s failed. No command Id available.\n%s' % (sql_stmt, e))
            else:
                raise AirflowException('run_sql call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s'
                                       % (sql_stmt, command.id, e))

    def monitor_command(self, command, sql_stmt):
        _command = command
        time.sleep(10)
        try:
            _command = Command.find(_command.id)
        except:
            time.sleep(30)
            _command = Command.find(_command.id)

        total_sleep_time = 0
        retries = 1000
        command_id = _command.id
        for i in range(retries):
            if _command.status == 'error':
                raise AirflowException('Insert statement failed: https://api.qubole.com/v2/analyze?command_id=%s\n %s'
                                       % (command_id, sql_stmt))
            elif Command.is_done(_command.status):
                return
            else:
                total_sleep_time += 10
                if total_sleep_time > self.expected_runtime * 1.5:
                    raise AirflowException(
                        "Total estimated runtime was exceeded, please adjust estimation in DAG if the process requires more time to complete query %s" % sql_stmt)
                time.sleep(10)
                _command = Command.find(command_id)

        raise AirflowException(
            'monitor_command call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s'
            % (sql_stmt, command.id))

    def etl(self):
        self.run_sql()


class SparkWrapper(object):
    def __init__(self, label, program, language, arguments, expected_runtime, dag_id, task_id, ds):
        Qubole.configure(api_token='%s' % os.environ['QUBOLE_API_TOKEN'])
        self.label = label
        self.program = program
        self.language = language
        self.arguments = arguments
        if expected_runtime == 0:
            self.expected_runtime = 7200  # 2 hour default
        else:
            self.expected_runtime = expected_runtime
        self.dag_id = dag_id
        self.task_id = task_id
        self.ds = ds

    def run_spark_job(self):
        command = None
        program = None
        try:
            template = Template(self.program)
            dwd = DataWarehouseDates()
            program = template.render(DS=self.ds,
                                      DS_TODAY=dwd.ds_today(self.ds),
                                      DS_DATE_ID=dwd.date_id_from_date_str(self.ds),
                                      DS_DATE_ADD=lambda days: dwd.date_add(self.ds, days),
                                      DS_TODAY_DATE_ADD=lambda days: dwd.date_add(dwd.ds_today(self.ds), days)
                                      )

            qubole_name = '%s_%s_%s' % (self.dag_id, self.task_id, self.ds)

            if 'SQL' in self.language.upper():
                program_with_tracking = '-- %s %s %s\n%s' % (self.dag_id, self.task_id, self.ds, program)
                command = SparkCommand.create(sql=program_with_tracking,
                                              arguments=self.arguments,
                                              label=self.label,
                                              name=qubole_name)
            elif 'PYTHON' in self.language.upper() or 'R' in self.language.upper():
                program_with_tracking = '# %s %s %s\n%s' % (self.dag_id, self.task_id, self.ds, program)
                command = SparkCommand.create(program=program_with_tracking,
                                              language=self.language,
                                              arguments=self.arguments,
                                              label=self.label,
                                              name=qubole_name)
            elif 'SCALA' in self.language.upper():
                program_with_tracking = '// %s %s %s\n%s' % (self.dag_id, self.task_id, self.ds, program)
                command = SparkCommand.create(program=program_with_tracking,
                                              language=self.language,
                                              arguments=self.arguments,
                                              label=self.label,
                                              name=qubole_name)
            else:
                raise AirflowException('Invalid Spark language specified.')

            self.monitor_command(command, program_with_tracking)
        except Exception as e:
            if command is None:
                raise AirflowException('run_sql call for %s failed. No command Id available.\n%s' % (sql_stmt, e))
            else:
                raise AirflowException('run_sql call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s'
                                       % (program, command.id, e))

    def monitor_command(self, command, program):
        _command = command
        time.sleep(10)
        try:
            _command = Command.find(_command.id)
        except:
            time.sleep(30)
            _command = Command.find(_command.id)

        total_sleep_time = 0
        retries = 1000
        command_id = _command.id
        for i in range(retries):
            if _command.status == 'error':
                raise AirflowException('Spark program failed: https://api.qubole.com/v2/analyze?command_id=%s\n %s'
                                       % (command_id, program))
            elif Command.is_done(_command.status):
                return
            else:
                total_sleep_time += 10
                if total_sleep_time > self.expected_runtime * 1.5:
                    raise AirflowException(
                        "Total estimated runtime for Spark job was exceeded, please adjust estimation in DAG if the process requires more time to complete query %s" % program)
                time.sleep(10)
                _command = Command.find(command_id)

        raise AirflowException(
            'Spark monitor_command call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s'
            % (sql_stmt, command.id))


class ExportToRDMS(object):
    def __init__(self, table_name, expected_runtime, dag_id, task_id):
        Qubole.configure(api_token='%s' % os.environ['QUBOLE_API_TOKEN'])
        self.table_name = table_name
        self.expected_runtime = expected_runtime
        self.dag_id = dag_id
        self.task_id = task_id
        self.host = os.environ['DB_HOST']
        self.user = os.environ['DB_USER']
        self.password = os.environ['DB_PASSWORD']
        self.db = os.environ['DB']
        self.s3_bucket = os.environ['S3_BUCKET']

        self.s3_resource = boto3.resource(
            's3',
            region_name='us-west-2',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

        self.s3_client = boto3.client(
            's3',
            region_name='us-west-2',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

    def monitor_command(self, command, sql_stmt):
        _command = command
        time.sleep(10)
        try:
            _command = Command.find(_command.id)
        except:
            time.sleep(30)
            _command = Command.find(_command.id)

        total_sleep_time = 0
        retries = 1000
        command_id = _command.id
        for i in range(retries):
            if _command.status == 'error':
                raise AirflowException('Statement failed: https://api.qubole.com/v2/analyze?command_id=%s\n %s'
                                       % (command_id, sql_stmt))
            elif Command.is_done(_command.status):
                return
            else:
                total_sleep_time += 10
                if total_sleep_time > self.expected_runtime * 1.5:
                    raise AirflowException(
                        "Total estimated runtime was exceeded, please adjust estimation in DAG if the process requires more time to complete query %s" % sql_stmt)
                time.sleep(10)
                _command = Command.find(command_id)

        raise AirflowException(
            'monitor_command call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s'
            % (sql_stmt, command.id))

    def get_table_columns(self):
        command = None
        sql = None
        try:
            sql = "show create table %s" % self.table_name
            command = HiveCommand.run(query=sql, label='default')
            buffer = io.BytesIO()
            command.get_results(fp=buffer)
            buffer.seek(0)

            buffer_list = [t for t in buffer]

            buffer.close()
            buffer = None

            column_names_types = []
            for l in buffer_list:
                decoded = l.decode("utf-8")
                column_type = decoded.split()
                if len(column_type) > 1:
                    if self.is_type(column_type[1].strip().strip(',').strip('(').strip(')')):
                        column_names_types.append(
                            (column_type[0]
                             .strip()
                             , column_type[1]
                             .strip(',')
                             .strip('(')
                             .strip(')')))

            return column_names_types

        except Exception as e:
            if command is None:
                raise AirflowException('get_table_columns call for %s failed. No command Id available.\n%s' %
                                       (sql, e))
            else:
                raise AirflowException(
                    'get_table_columns call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s' % (
                        sql, command.id, e))

    def create_temp_hive_table(self, columns):
        command = None
        create_table_stmt = None
        try:
            create_table_stmt = """
            SET hive.exec.compress.output=false;
            
            DROP TABLE IF EXISTS %s_airflow_temp;  
            
            CREATE EXTERNAL TABLE %s_airflow_temp(
            %s)
            COMMENT 'The table %s_airflow_temp was generated by Hadoop Result Caching'
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            STORED AS TEXTFILE
            LOCATION 's3n://%s/result_cache/%s_airflow_temp/'
            TBLPROPERTIES("orc.compress"="NONE");""" % \
                                (self.table_name,
                                 self.table_name,
                                 ',\n'.join(['%s     %s' % (c[0], c[1]) for c in columns]),
                                 self.table_name,
                                 self.s3_bucket,
                                 self.table_name
                                 )
            command = HiveCommand.run(query=create_table_stmt, label='default')
            self.monitor_command(command, create_table_stmt)
        except Exception as e:
            if command is None:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. No command Id available. \n %s' %
                    (create_table_stmt, e))
            else:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s \n %s' %
                    (create_table_stmt, command.id, e))

    def copy_to_temp_hive(self, columns):
        command = None
        copy_table_stmt = None
        try:
            copy_table_stmt = '''SET hive.exec.compress.output=false; 
                INSERT INTO %s_airflow_temp select * from %s''' % (self.table_name, self.table_name)

            command = HiveCommand.run(query=copy_table_stmt, label='default')
            self.monitor_command(command, copy_table_stmt)
        except Exception as e:
            if command is None:
                raise AirflowException(
                    'copy_to_temp_hive call for %s failed. No command Id available. \n%s' % (copy_table_stmt, e))
            else:
                raise AirflowException(
                    'copy_to_temp_hive call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s' % (
                        copy_table_stmt, command.id, e))

    def map_sql_types(self, columns):
        type_mappings = {'TINYINT': 'TINYINT',
                         'SMALLINT': 'SMALLINT',
                         'INT': 'INT',
                         'INTEGER': 'INT',
                         'BIGINT': 'BIGINT',
                         'FLOAT': 'FLOAT',
                         'DOUBLE': 'DOUBLE',
                         # 'DOUBLE PRECISION': 'DOUBLE PRECISION',
                         # 'DECIMAL': '',
                         # 'NUMERIC': '',
                         'TIMESTAMP': 'TIMESTAMP',
                         'DATE': 'DATE',
                         # 'INTERVAL': '',
                         'STRING': 'TEXT',
                         # 'VARCHAR': '',
                         # 'CHAR': '',
                         'BOOLEAN': 'BOOLEAN'}
        try:
            destination_columns = []
            for c in columns:
                t = (c[0], type_mappings[c[1].upper()])
                destination_columns.append(t)

        except Exception:
            raise AirflowException('Unsupported type in my_sql_types')

        return destination_columns

    def is_type(self, value):
        types = ['TINYINT',
                 'SMALLINT',
                 'INT',
                 'INTEGER',
                 'BIGINT',
                 'FLOAT',
                 'DOUBLE',
                 'DOUBLE PRECISION',
                 'DECIMAL',
                 'NUMERIC',
                 'TIMESTAMP',
                 'DATE',
                 'INTERVAL',
                 'STRING',
                 'VARCHAR',
                 'CHAR',
                 'BOOLEAN']

        if value.upper() in types:
            return True
        else:
            return False

    def create_output_table(self, columns):
        try:
            connection = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db)

            table_name = self.table_name.split('.')[1]

            sql = """
            DROP TABLE IF EXISTS %s_airflow_temp;
            
            CREATE TABLE %s_airflow_temp (
            %s
            )""" % (table_name,
                    table_name,
                    ',\n'.join(['%s     %s' % (c[0], c[1]) for c in self.map_sql_types(columns)]),)

            with connection.cursor() as cursor:
                cursor.execute(sql)

            connection.close()

        except Exception as e:
            raise AirflowException(
                'create_output_table call for %s failed.\n%s' % (self.table_name, e))

    def bulk_load_cache_table(self):
        prefix = 'result_cache/%s_airflow_temp' % self.table_name

        try:
            paginator = self.s3_client.get_paginator('list_objects')
            result_iterator = paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix)
            csv_files_to_load = []
            for o in result_iterator:
                for content in o.get('Contents'):
                    csv_files_to_load.append(content['Key'])

            connection = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db)

            for csv in csv_files_to_load:
                sql = """LOAD DATA FROM S3 FILE 's3-us-west-2://%s/%s'
                            INTO TABLE %s_airflow_temp
                            FIELDS TERMINATED BY ','
                                LINES TERMINATED BY '\\n'
    
                            """ % (self.s3_bucket, csv, self.table_name.split('.')[1])

                with connection.cursor() as cursor:
                    cursor.execute(sql)

            connection.commit()
            connection.close()
        except Exception as e:
            raise AirflowException('bulk_load_cache_table call for %s failed.\n%s' % (self.table_name, e))

    def rename_temp_cache_table(self):
        sql = None
        try:
            connection = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db)

            sql = """DROP TABLE IF EXISTS %s;
                     RENAME TABLE %s_airflow_temp TO %s;""" % (self.table_name.split('.')[1],
                                                               self.table_name.split('.')[1],
                                                               self.table_name.split('.')[1])
            with connection.cursor() as cursor:
                cursor.execute(sql)

            connection.commit()
            connection.close()
        except Exception as e:
            raise AirflowException('bulk_load_cache_table call for %s failed.\n%s\n%s' % (self.table_name, sql, e))

    def drop_temp_hive_table(self):
        command = None
        create_table_stmt = None
        try:
            create_table_stmt = "DROP TABLE IF EXISTS %s_airflow_temp;" % self.table_name

            command = HiveCommand.run(query=create_table_stmt, label='default')
            self.monitor_command(command, create_table_stmt)
        except Exception as e:
            if command is None:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. No command Id available.\n%s' % (create_table_stmt, e))
            else:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s' % (
                        create_table_stmt, command.id, e))

        try:
            stmt = "s3cmd -c /usr/lib/hustler/s3cfg rm -rf s3://%s/result_cache/%s_airflow_temp/;" % \
                   (self.s3_bucket, self.table_name)

            command = Command.run(command_type='ShellCommand', inline=stmt, label='default')
            self.monitor_command(command, stmt)
        except Exception as e:
            if command is None:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. No command Id available.\n%s' % (create_table_stmt, e))
            else:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s' % (
                        create_table_stmt, command.id, e))

    def export(self):
        columns = self.get_table_columns()
        self.drop_temp_hive_table()  # Done just in case a failure occurred
        self.create_temp_hive_table(columns)
        self.copy_to_temp_hive(columns)
        self.create_output_table(columns)
        self.bulk_load_cache_table()
        self.rename_temp_cache_table()
        self.drop_temp_hive_table()


class ExportToRedshift(object):
    def __init__(self, table_name, expected_runtime, dag_id, task_id):
        Qubole.configure(api_token='%s' % os.environ['QUBOLE_API_TOKEN'])
        self.table_name = table_name
        self.expected_runtime = expected_runtime
        self.dag_id = dag_id
        self.task_id = task_id
        self.host = os.environ['RS_HOST']
        self.port = os.environ['RS_PORT']
        self.user = os.environ['RS_USER']
        self.password = os.environ['RS_PASSWORD']
        self.db = os.environ['RS_DB']
        self.s3_bucket = os.environ['S3_BUCKET']
        self.rs_s3_auth = os.environ['RS_S3_AUTH']
        self.connection_string = 'dbname=%s host=%s port=%s user=%s password=%s connect_timeout=1200' % \
                                 (self.db, self.host, self.port, self.user, self.password)

        self.s3_resource = boto3.resource(
            's3',
            region_name='us-west-2',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

        self.s3_client = boto3.client(
            's3',
            region_name='us-west-2',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

    def monitor_command(self, command, sql_stmt):
        _command = command
        time.sleep(10)
        try:
            _command = Command.find(_command.id)
        except:
            time.sleep(30)
            _command = Command.find(_command.id)

        total_sleep_time = 0
        retries = 1000
        command_id = _command.id
        for i in range(retries):
            if _command.status == 'error':
                raise AirflowException('Statement failed: https://api.qubole.com/v2/analyze?command_id=%s\n %s'
                                       % (command_id, sql_stmt))
            elif Command.is_done(_command.status):
                return
            else:
                total_sleep_time += 10
                if total_sleep_time > self.expected_runtime * 1.5:
                    raise AirflowException(
                        "RS Total estimated runtime was exceeded, please adjust estimation in DAG if the process requires more time to complete query %s" % sql_stmt)
                time.sleep(10)
                _command = Command.find(command_id)

        raise AirflowException(
            'RS_monitor_command call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s'
            % (sql_stmt, command.id))

    def get_table_columns(self):
        command = None
        sql = None
        try:
            sql = "show create table %s" % self.table_name
            command = HiveCommand.run(query=sql, label='default')
            buffer = io.BytesIO()
            command.get_results(fp=buffer)
            buffer.seek(0)

            buffer_list = [t for t in buffer]

            buffer.close()
            buffer = None

            column_names_types = []
            for l in buffer_list:
                decoded = l.decode("utf-8")
                column_type = decoded.split()
                if len(column_type) > 1:
                    if self.is_type(column_type[1].strip().strip(',').strip('(').strip(')')):
                        column_names_types.append(
                            (column_type[0]
                             .strip()
                             , column_type[1]
                             .strip(',')
                             .strip('(')
                             .strip(')')))

            return column_names_types

        except Exception as e:
            if command is None:
                raise AirflowException('RS_get_table_columns call for %s failed. No command Id available.\n%s' %
                                       (sql, e))
            else:
                raise AirflowException(
                    'RS_get_table_columns call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s' % (
                        sql, command.id, e))

    def create_temp_hive_table(self, columns):
        command = None
        create_table_stmt = None
        try:
            create_table_stmt = """
            SET hive.exec.compress.output=false;

            DROP TABLE IF EXISTS %s_airflow_temp;  

            CREATE EXTERNAL TABLE %s_airflow_temp(
            %s)
            COMMENT 'The table %s_airflow_temp was generated by Hadoop Result Caching'
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            STORED AS TEXTFILE
            LOCATION 's3n://%s/result_cache/%s_airflow_temp/'
            TBLPROPERTIES("orc.compress"="NONE");""" % \
                                (self.table_name,
                                 self.table_name,
                                 ',\n'.join(['%s     %s' % (c[0], c[1]) for c in columns]),
                                 self.table_name,
                                 self.s3_bucket,
                                 self.table_name
                                 )

            command = HiveCommand.run(query=create_table_stmt, label='default')
            self.monitor_command(command, create_table_stmt)
        except Exception as e:
            if command is None:
                raise AirflowException(
                    'RS_create_temp_hive_table call for %s failed. No command Id available. \n %s' %
                    (create_table_stmt, e))
            else:
                raise AirflowException(
                    'RS_create_temp_hive_table call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s \n %s' %
                    (create_table_stmt, command.id, e))

    def copy_to_temp_hive(self, columns):
        command = None
        copy_table_stmt = None
        try:
            copy_table_stmt = '''SET hive.exec.compress.output=false; 
                INSERT INTO %s_airflow_temp select * from %s''' % (self.table_name, self.table_name)

            command = HiveCommand.run(query=copy_table_stmt, label='default')
            self.monitor_command(command, copy_table_stmt)
        except Exception as e:
            if command is None:
                raise AirflowException(
                    'RS_copy_to_temp_hive call for %s failed. No command Id available. \n%s' % (copy_table_stmt, e))
            else:
                raise AirflowException(
                    'RS_copy_to_temp_hive call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s' % (
                        copy_table_stmt, command.id, e))

    def map_sql_types(self, columns):
        type_mappings = {'TINYINT': 'SMALLINT',
                         'SMALLINT': 'SMALLINT',
                         'INT': 'INTEGER',
                         'INTEGER': 'INTEGER',
                         'BIGINT': 'BIGINT',
                         'FLOAT': 'DOUBLE PRECISION',
                         'DOUBLE': 'DOUBLE PRECISION',
                         # 'DOUBLE PRECISION': 'DOUBLE PRECISION',
                         # 'DECIMAL': '',
                         # 'NUMERIC': '',
                         'TIMESTAMP': 'TIMESTAMP',
                         'DATE': 'DATE',
                         # 'INTERVAL': '',
                         'STRING': 'TEXT',
                         # 'VARCHAR': '',
                         # 'CHAR': '',
                         'BOOLEAN': 'BOOLEAN'}
        try:
            destination_columns = []
            for c in columns:
                t = (c[0], type_mappings[c[1].upper()])
                destination_columns.append(t)

        except Exception:
            raise AirflowException('RS_Unsupported type in Redshift_types')

        return destination_columns

    def is_type(self, value):
        types = ['TINYINT',
                 'SMALLINT',
                 'INT',
                 'INTEGER',
                 'BIGINT',
                 'FLOAT',
                 'DOUBLE',
                 'DOUBLE PRECISION',
                 'DECIMAL',
                 'NUMERIC',
                 'TIMESTAMP',
                 'DATE',
                 'INTERVAL',
                 'STRING',
                 'VARCHAR',
                 'CHAR',
                 'BOOLEAN']

        if value.upper() in types:
            return True
        else:
            return False

    def create_output_table(self, columns):
        try:
            connection = psycopg2.connect(self.connection_string)

            table_name = self.table_name.split('.')[1]

            sql = """
            DROP TABLE IF EXISTS shared_ga.%s_airflow_temp;

            CREATE TABLE shared_ga.%s_airflow_temp (
            %s
            );""" % (table_name,
                     table_name,
                     ',\n'.join(['%s     %s' % (c[0], c[1]) for c in self.map_sql_types(columns)]),)
            sql = sql.replace('`', '"')

            with connection.cursor() as cursor:
                cursor.execute(sql)

            connection.commit()
            connection.close()

        except Exception as e:
            raise AirflowException(
                'RS_create_output_table call for %s failed.\n%s' % (self.table_name, e))

    def bulk_load_cache_table(self):
        prefix = 'result_cache/%s_airflow_temp' % self.table_name

        try:
            paginator = self.s3_client.get_paginator('list_objects')
            result_iterator = paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix)
            csv_files_to_load = []
            for o in result_iterator:
                for content in o.get('Contents'):
                    csv_files_to_load.append(content['Key'])

            connection = psycopg2.connect(self.connection_string)

            for csv in csv_files_to_load:
                sql = """COPY shared_ga.%s_airflow_temp
                            FROM 's3://%s/%s'
                            iam_role '%s'
                            region 'us-west-2'
                            FORMAT AS CSV;""" % (self.table_name.split('.')[1],
                                                 self.s3_bucket, csv, self.rs_s3_auth)

                with connection.cursor() as cursor:
                    cursor.execute(sql)

            connection.commit()
            connection.close()
        except Exception as e:
            raise AirflowException('RS_bulk_load_cache_table call for %s failed.\n%s' % (self.table_name, e))

    def rename_temp_cache_table(self):
        sql = None
        try:
            connection = psycopg2.connect(self.connection_string)

            sql = """DROP TABLE IF EXISTS shared_ga.%s;
                     ALTER TABLE shared_ga.%s_airflow_temp RENAME TO %s;
                     GRANT SELECT ON shared_ga.%s TO GROUP service_tableau;""" % (self.table_name.split('.')[1],
                                                                                  self.table_name.split('.')[1],
                                                                                  self.table_name.split('.')[1],
                                                                                  self.table_name.split('.')[1])

            with connection.cursor() as cursor:
                cursor.execute(sql)

            connection.commit()
            connection.close()
        except Exception as e:
            raise AirflowException('RS_bulk_load_cache_table call for %s failed.\n%s\n%s' % (self.table_name, sql, e))

    def drop_temp_hive_table(self):
        command = None
        create_table_stmt = None
        try:
            create_table_stmt = "DROP TABLE IF EXISTS %s_airflow_temp;" % self.table_name

            command = HiveCommand.run(query=create_table_stmt, label='default')
            self.monitor_command(command, create_table_stmt)
        except Exception as e:
            if command is None:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. No command Id available.\n%s' % (create_table_stmt, e))
            else:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s' % (
                        create_table_stmt, command.id, e))

        try:
            stmt = "s3cmd -c /usr/lib/hustler/s3cfg rm -rf s3://%s/result_cache/%s_airflow_temp/;" % \
                   (self.s3_bucket, self.table_name)

            command = Command.run(command_type='ShellCommand', inline=stmt, label='default')
            self.monitor_command(command, stmt)
        except Exception as e:
            if command is None:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. No command Id available.\n%s' % (create_table_stmt, e))
            else:
                raise AirflowException(
                    'create_temp_hive_table call for %s failed. https://api.qubole.com/v2/analyze?command_id=%s\n%s' % (
                        create_table_stmt, command.id, e))

    def export(self):
        columns = self.get_table_columns()
        self.drop_temp_hive_table()  # Done just in case a failure occurred
        self.create_temp_hive_table(columns)
        self.copy_to_temp_hive(columns)
        self.create_output_table(columns)
        self.bulk_load_cache_table()
        self.rename_temp_cache_table()
        self.drop_temp_hive_table()


def qubole_wrapper(templates_dict, *args, **kwargs):
    qw = QuboleWrapper(
        kwargs['db_type'],
        kwargs['raw_sql'],
        kwargs['expected_runtime'],
        kwargs['dag_id'],
        kwargs['task_id'],
        templates_dict['ds'])
    qw.etl()


def spark_wrapper(templates_dict, *args, **kwargs):
    sw = SparkWrapper(kwargs['label'],
                      kwargs['program'],
                      kwargs['language'],
                      kwargs['arguments'],
                      kwargs['expected_runtime'],
                      kwargs['dag_id'],
                      kwargs['task_id'],
                      templates_dict['ds'])
    sw.run_spark_job()


def export_to_rdms(templates_dict, *args, **kwargs):
    export = ExportToRDMS(
        kwargs['table_name'],
        kwargs['expected_runtime'],
        kwargs['dag_id'],
        kwargs['task_id'])
    export.export()


def export_to_redshift(templates_dict, *args, **kwargs):
    export = ExportToRedshift(
        kwargs['table_name'],
        kwargs['expected_runtime'],
        kwargs['dag_id'],
        kwargs['task_id'])
    export.export()

#
# if __name__ == "__main__":
#     export = ExportToRDMS(
#         'as_kf.bo3_mtx_mp_users_wau',
#         3000,
#         'stub dag id',
#         'stub dag id')
#     export.export()


# if __name__ == "__main__":
#     export = ExportToRedshift(
#         'as_kf.bo3_mtx_mp_users_wau',
#         3000,
#         'stub dag id',
#         'stub dag id')
#     export.export()

# if __name__ == "__main__":
#     spark = SparkWrapper(label='spark',
#                          program=spark_com,
#                          language='scala',
#                          arguments='--num-executors 90 --max-executors 179 --executor-memory 37G --executor-cores 5 --conf spark.executor.instances=5 --driver-memory 20g',
#                          expected_runtime=0,
#                          dag_id='test',
#                          task_id='test',
#                          ds="2017-10-1")
#     spark.run_spark_job()
