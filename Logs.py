import inspect
import pymysql
from datetime import datetime
from ShareInfo import MysqlInfo


class __DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()
        self.closedb = True

    def __del__(self):
        try:
            if not self.closedb:
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    self.conn.close()
                self.closedb = True
        except (pymysql.Error, Exception):
            pass


class DownLog(__DatabaseManager):
    def __init__(self, mysql_info, ftp_name):
        super().__init__()
        self.errlog = ErrorLog(mysql_info)
        self.mysqlinfo = mysql_info
        self.ftp_name = ftp_name
        self.mysqlinfo.db_name = mysql_info.db_name or 'mroparse'
        self.mysqlinfo.tb_name = "DownLog"
        self._connect()
        self.closedb = False
        print("DL", self.mysqlinfo.tb_name)

    def _connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.mysqlinfo.host,
                port=self.mysqlinfo.port,
                user=self.mysqlinfo.user,
                password=self.mysqlinfo.passwd,
                database=self.mysqlinfo.db_name,
                autocommit=True
            )
            self.cursor = self.conn.cursor()
            self._create_table()
        except pymysql.Error as e:
            self.cursor, self.conn = None, None
            self.errlog.add_error(e)

    def _create_table(self):
        try:
            self.cursor.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_name='{self.mysqlinfo.tb_name}'")
            if not self.cursor.fetchone():
                self.cursor.execute(f"CREATE TABLE {self.mysqlinfo.tb_name} ("
                                    f"id INT PRIMARY KEY AUTO_INCREMENT, "
                                    f"ftp_name VARCHAR(255) NOT NULL, "
                                    f"filepath VARCHAR(255) NOT NULL, "
                                    f"log_time DATETIME NOT NULL)")

                # self.cursor.execute(f"CREATE INDEX filepath_index ON {self.mysqlinfo.tb_name} (filepath)")
                # self.cursor.execute(f"CREATE INDEX ftp_name_index ON {self.mysqlinfo.tb_name} (ftp_name)")
                self.conn.commit()

        except pymysql.Error as e:
            self.errlog.add_error(e)

    def isexists(self, filepath):
        if not self.cursor:
            self._connect()

        try:
            print(f"SELECT * FROM {self.mysqlinfo.tb_name} WHERE ftp_name = %s AND filepath = %s",
                                (self.ftp_name, filepath))
            self.cursor.execute(f"SELECT * FROM {self.mysqlinfo.tb_name} WHERE ftp_name = %s AND filepath = %s",
                                (self.ftp_name, filepath))
            result = self.cursor.fetchone()
            return bool(result) if result else False
        except pymysql.Error as e:
            print(self.ftp_name, self.mysqlinfo.tb_name, self.mysqlinfo.db_name)
            self.errlog.add_error(e)
            return False

    def savelog(self, filepath):
        if not self.cursor:
            self._connect()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.cursor.execute(
                f"INSERT INTO {self.mysqlinfo.tb_name} (ftp_name, filepath, log_time) VALUES (%s, %s, %s)",
                (self.ftp_name, filepath, now))
        except pymysql.IntegrityError:
            return True
        except pymysql.Error as e:
            self.errlog.add_error(e)
            return False
        return True

    def dellog_by_time(self, time=None):
        if not self.cursor:
            self._connect()
        if time is None:
            time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.cursor.execute(f"DELETE FROM {self.mysqlinfo.tb_name} WHERE log_time < %s", (time,))
        except pymysql.Error as e:
            self.errlog.add_error(e)
            return False
        return True


class ErrorLog:
    def __init__(self, mysql_info: MysqlInfo):
        self.mysql_info = mysql_info
        self.mysql_info.db_name = mysql_info.db_name or 'mroparse'
        self.mysql_info.tb_name = 'ErrorLog'
        self._connect()
        self.closedb = False
    def _connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.mysql_info.host,
                port=self.mysql_info.port,
                user=self.mysql_info.user,
                password=self.mysql_info.passwd,
                database=self.mysql_info.db_name
            )
            self.cursor = self.conn.cursor()
            self._create_table()
        except pymysql.Error as e:
            self.cursor = None
            self.conn = None
            raise Exception(f"Error connecting to MySQL: {e}")

    def _create_table(self):
        try:
            self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.mysql_info.tb_name} ("
                                "id INTEGER PRIMARY KEY AUTO_INCREMENT, "
                                "log_time DATETIME NOT NULL, "
                                "from_class VARCHAR(255) NOT NULL, "
                                "from_func VARCHAR(255) NOT NULL,"
                                "error_text TEXT NOT NULL)")
            self.conn.commit()
            try:
                self.cursor.execute(f"""
                            CREATE INDEX log_time_index ON {self.mysql_info.tb_name} (log_time);
                        """)
                self.conn.commit()
            except pymysql.Error:
                pass
        except pymysql.Error as e:
            raise Exception(f"Error creating table: {e}")

    def add_error(self, error_text):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        caller = inspect.stack()[1]
        caller_class = caller[0].f_locals.get('self').__class__.__name__
        caller_method = caller[0].f_code.co_name

        try:
            if not self.cursor:
                self._connect()

            self.cursor.execute(
                f"INSERT INTO {self.mysql_info.tb_name} (log_time, from_class, from_func, error_text) "
                f"VALUES (%s, %s, %s, %s)",
                (now, caller_class, caller_method, str(error_text).replace("'", "‘")))
            self.conn.commit()
        except pymysql.Error as e:
            raise Exception(f"Error adding error record: {e}")

    def del_errors_before(self, start_time, end_time):
        try:
            if not self.cursor:
                self._connect()
            self.cursor.execute(f"DELETE FROM {self.mysql_info.tb_name} WHERE log_time >= %s "
                                f"AND log_time <= %s", (start_time, end_time))
            self.conn.commit()
        except pymysql.Error as e:
            raise Exception(f"Error deleting error records before time: {e}")

    def get_errors_by_time(self, start_time, end_time):
        try:
            if not self.cursor:
                self._connect()
            self.cursor.execute(f"SELECT * FROM {self.mysql_info.tb_name} WHERE log_time >= %s AND log_time <= %s",
                                (start_time, end_time))
            results = self.cursor.fetchall()
            return results
        except pymysql.Error as e:
            raise Exception(f"Error querying error records by time: {e}")
