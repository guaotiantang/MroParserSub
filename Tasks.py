import datetime
import pymysql
import aiomysql
from contextlib import closing
from pymysql.cursors import DictCursor

from ShareInfo import MysqlInfo
from Logs import ErrorLog


class Task:
    def __init__(self, mysql_info: MysqlInfo):
        self.host = mysql_info.host
        self.port = mysql_info.port
        self.user = mysql_info.user
        self.passwd = mysql_info.passwd
        self.db_name = mysql_info.db_name or 'mroparse'
        self.tb_name = 'mrotasks'
        self.errlog = ErrorLog(mysql_info)
        self._initialize_database()

    def _connect(self):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            db=self.db_name,
            cursorclass=DictCursor,
            autocommit=False
        )

    def _initialize_database(self):
        with closing(self._connect()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.db_name} (
                        task_id INT AUTO_INCREMENT PRIMARY KEY,
                        main_zip VARCHAR(255) NOT NULL,
                        sub_zip_path VARCHAR(255) NOT NULL,
                        xml_file VARCHAR(255) NOT NULL,
                        task_status ENUM('unparse','locked' , 'parsing', 'parsed') NOT NULL,
                        uptime TIMESTAMP NOT NULL,
                        ftp_name VARCHAR(255) NOT NULL
                    )
                """)

                for index_name, column_name in [('idx_task_id', 'task_id'),
                                                ('idx_ftp_name', 'ftp_name'),
                                                ('idx_task_status', 'task_status')]:
                    cursor.execute(f"SHOW INDEX FROM {self.db_name} WHERE KEY_NAME = '{index_name}'")
                    if cursor.rowcount == 0:
                        cursor.execute(f"CREATE INDEX {index_name} ON {self.db_name}({column_name})")

            conn.commit()

    @staticmethod
    def _execute_update(conn, query):
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

    def tasks_add(self, tasks, ftp_name):
        with closing(self._connect()) as conn:
            try:
                for task in tasks:
                    main_zip = task['main'].replace('\\', '\\\\')
                    sub_zip_path = task['path'].replace('\\', '\\\\')
                    query = f"""
                        INSERT INTO {self.db_name} 
                        (main_zip, sub_zip_path, xml_file, task_status, uptime, ftp_name) 
                        SELECT '{main_zip}', '{sub_zip_path}', '{task['xml_file']}', 'unparse', 
                        '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', '{ftp_name}'
                        FROM dual 
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {self.db_name} 
                            WHERE main_zip='{main_zip}' AND sub_zip_path='{sub_zip_path}' AND ftp_name='{ftp_name}'
                        )
                    """
                    self._execute_update(conn, query)
            except Exception as e:
                conn.rollback()
                raise e

    def tasks_update(self, task_id, status):
        try:
            with closing(self._connect()) as conn:
                with conn.cursor() as cursor:
                    query = f"""
                        UPDATE {self.db_name} 
                        SET task_status='{status}', uptime='{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' 
                        WHERE task_id={task_id}
                    """
                    cursor.execute(query)
                conn.commit()
        except Exception as e:
            conn.rollback()
            self.errlog.add_error(e)


class MroTaskAsync:
    def __init__(self, mysql_info: MysqlInfo):
        self.host = mysql_info.host
        self.port = mysql_info.port
        self.user = mysql_info.user
        self.passwd = mysql_info.passwd
        self.db_name = mysql_info.db_name or 'mroparse'
        self.tb_name = mysql_info.tb_name or 'mrotasks'

    async def _connect(self):
        return await aiomysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.passwd,
            db=self.db_name,
            autocommit=False
        )

    async def tasks_get(self, task_num, ftp_name):
        tasks = []
        try:
            async with self._connect() as conn:
                async with conn.cursor() as cursor:
                    query = f"""
                        SELECT * FROM {self.db_name} 
                        WHERE task_status='unparse' AND ftp_name='{ftp_name}' 
                        LIMIT {task_num} 
                        FOR UPDATE
                    """
                    await cursor.execute(query)
                    raw_tasks = await cursor.fetchall()

                    for task in raw_tasks:
                        query = f"""
                            UPDATE {self.db_name} 
                            SET task_status='parsing', uptime='{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' 
                            WHERE task_id={task['task_id']}
                        """
                        await cursor.execute(query)
                    await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
        return tasks
