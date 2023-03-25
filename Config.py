import os
import ftputil
import pymysql
import datetime
import configparser
from datetime import datetime

from ftputil.error import FTPOSError


class MysqlInfo:
    def __init__(self, cfg_path=os.path.join(os.getcwd(), 'configure', 'mysql.ini'), section="LocalServer"):
        self.__cfg_path = cfg_path
        os.makedirs(os.path.dirname(self.__cfg_path), exist_ok=True)
        self.__config = configparser.ConfigParser()
        self.section = section
        try:
            if not os.path.exists(self.__cfg_path):
                self.__config[self.section] = {
                    'host': '',
                    'port': -1,
                    'user': '',
                    'passwd': ''
                }
                with open(self.__cfg_path, 'w') as f:
                    self.__config.write(f)
            self.__config.read(self.__cfg_path)
            self.host = self.__config.get(self.section, 'host')
            self.port = int(self.__config.get(self.section, 'port'))
            self.user = self.__config.get(self.section, 'user')
            self.passwd = self.__config.get(self.section, 'passwd')
            self.db_name = None
            self.tb_name = None
        except (FileNotFoundError, configparser.Error, ValueError) as e:
            raise Exception(f"Error initializing MysqlInfo: {e}")

    def update(self, host=None, port=None, user=None, passwd=None):
        try:
            self.__config.set(self.section, 'host', host)
        except configparser.NoSectionError:
            self.__config.read(self.__cfg_path)
        try:

            if host is not None:
                self.__config.set(self.section, 'host', host)
                self.host = host
            if port is not None:
                self.__config.set(self.section, 'port', str(port))
                self.port = int(port)
            if user is not None:
                self.__config.set(self.section, 'user', user)
                self.user = user
            if passwd is not None:
                self.__config.set(self.section, 'passwd', passwd)
                self.passwd = passwd

            with open(self.__cfg_path, 'w') as f:
                self.__config.write(f)
        except (FileNotFoundError, configparser.Error, ValueError):
            return False
        return True

    def read(self):
        try:
            self.__config.read(self.__cfg_path)
            self.host = self.__config.get(self.section, 'host')
            self.port = int(self.__config.get(self.section, 'port'))
            self.user = self.__config.get(self.section, 'user')
            self.passwd = self.__config.get(self.section, 'passwd')
        except (configparser.Error, ValueError):
            return False
        return True

    def check(self):
        try:
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.passwd,
                database=self.db_name
            )
            conn.close()
        except (pymysql.Error, Exception):
            return False
        return True


class FTPInfo:
    def __init__(self, cfg_path=os.path.join(os.getcwd(), 'configure', 'ftpinfo.ini'), cfg_name='FTPInfo'):
        self.__cfg_path = cfg_path
        self.__cfg_name = cfg_name
        os.makedirs(os.path.dirname(cfg_path), exist_ok=True)
        self.__config = configparser.ConfigParser()
        try:
            if not os.path.exists(cfg_path):
                self.__config[self.__cfg_name] = {
                    'ftp_name': '',
                    'host': '',
                    'port': -1,
                    'user': '',
                    'passwd': '',
                    'sync_path': '',
                    'down_path': '',
                    'scan_filter': ''
                }
                with open(cfg_path, 'w') as f:
                    self.__config.write(f)
            self.__config.read(cfg_path)
            self.ftp_name = self.__config.get(self.__cfg_name, 'ftp_name')
            self.host = self.__config.get(self.__cfg_name, 'host')
            self.port = int(self.__config.get(self.__cfg_name, 'port'))
            self.user = self.__config.get(self.__cfg_name, 'user')
            self.passwd = self.__config.get(self.__cfg_name, 'passwd')
            self.sync_path = self.__config.get(self.__cfg_name, 'sync_path')
            self.down_path = self.__config.get(self.__cfg_name, 'down_path')
            self.scan_filter = self.__config.get(self.__cfg_name, 'scan_filter')
        except (configparser.Error, Exception) as e:
            raise Exception(e)

    def update(self, ftp_name=None, host=None, port=None, user=None, passwd=None, sync_path=None, down_path=None,
               scan_filter=None):
        if ftp_name is not None:
            self.__config.set(self.__cfg_name, 'ftp_name', ftp_name)
            self.ftp_name = ftp_name
        if host is not None:
            self.__config.set(self.__cfg_name, 'host', host)
            self.host = host
        if port is not None:
            self.__config.set(self.__cfg_name, 'port', str(port))
            self.port = int(port)
        if user is not None:
            self.__config.set(self.__cfg_name, 'user', user)
            self.user = user
        if passwd is not None:
            self.__config.set(self.__cfg_name, 'passwd', passwd)
            self.passwd = passwd
        if sync_path is not None:
            self.__config.set(self.__cfg_name, 'sync_path', sync_path)
            self.sync_path = sync_path
        if down_path is not None:
            self.__config.set(self.__cfg_name, 'down_path', down_path)
            self.down_path = down_path
        if scan_filter is not None:
            self.__config.set(self.__cfg_name, 'scan_filter', scan_filter)
            self.scan_filter = scan_filter
        try:
            with open(self.__cfg_path, 'w') as f:
                self.__config.write(f)
        except configparser.Error:
            return False
        return True

    def read(self):
        try:
            self.__config.read(self.__cfg_path)
            self.ftp_name = self.__config.get(self.__cfg_name, 'ftp_name')
            self.host = self.__config.get(self.__cfg_name, 'host')
            self.port = int(self.__config.get(self.__cfg_name, 'port'))
            self.user = self.__config.get(self.__cfg_name, 'user')
            self.passwd = self.__config.get(self.__cfg_name, 'passwd')
            self.sync_path = self.__config.get(self.__cfg_name, 'sync_path')
            self.down_path = self.__config.get(self.__cfg_name, 'down_path')
            self.scan_filter = self.__config.get(self.__cfg_name, 'scan_filter')
        except (configparser.Error, ValueError):
            return False
        return True

    def check(self):
        try:
            ftp = ftputil.FTPHost(self.host, self.user, self.passwd, self.port, timeout=60)
            ftp.close()
        except (FTPOSError, Exception):
            return False
        return True


class PerfInfo:
    def __init__(self, cfg_path=os.path.join(os.getcwd(), 'configure', 'performance.ini'), cfg_name='Performance'):
        self.__cfg_path = cfg_path
        self.__cfg_name = cfg_name
        os.makedirs(os.path.dirname(cfg_path), exist_ok=True)
        self.__config = configparser.ConfigParser()
        try:
            if not os.path.exists(cfg_path):
                self.__config[self.__cfg_name] = {
                    'processes': '4',
                    'threads': '4'
                }
                with open(cfg_path, 'w') as f:
                    self.__config.write(f)
            self.__config.read(cfg_path)
            self.processes = int(self.__config.get(self.__cfg_name, 'processes'))
            self.threads = int(self.__config.get(self.__cfg_name, 'threads'))
        except (configparser.Error, Exception) as e:
            raise Exception(e)

    def update(self, processes=None, threads=None):
        if processes is not None:
            self.__config.set(self.__cfg_name, 'processes', str(processes))
            self.processes = int(processes)
        if threads is not None:
            self.__config.set(self.__cfg_name, 'threads', str(threads))
            self.threads = int(threads)
        try:
            with open(self.__cfg_path, 'w') as f:
                self.__config.write(f)
        except configparser.Error:
            return False
        return True

    def read(self):
        try:
            self.__config.read(self.__cfg_path)
            self.processes = int(self.__config.get(self.__cfg_name, 'processes'))
            self.threads = int(self.__config.get(self.__cfg_name, 'threads'))
        except (configparser.Error, ValueError):
            return False
        return True


class SubOSInfo:
    def __init__(self, cfg_path=os.path.join(os.getcwd(), 'configure', 'subosinfo.ini'),
                 cfg_name='MroParserSub'):
        self.__cfg_path = cfg_path
        self.__cfg_name = cfg_name
        os.makedirs(os.path.dirname(cfg_path), exist_ok=True)
        self.__config = configparser.ConfigParser()
        try:
            if not os.path.exists(cfg_path):
                self.__config[self.__cfg_name] = {
                    'name': 'MroParserSub',
                    'version': '1.0',
                    'install_date': str(datetime.date.today()),
                    'update_date': str(datetime.date.today())
                }
                with open(cfg_path, 'w') as f:
                    self.__config.write(f)
            self.__config.read(cfg_path)
            self.name = self.__config.get(self.__cfg_name, 'name')
            self.version = self.__config.get(self.__cfg_name, 'version')
            self.install_date = datetime.datetime.strptime(self.__config.get(self.__cfg_name, 'install_date'),
                                                           '%Y-%m-%d').date()
            self.update_date = datetime.datetime.strptime(self.__config.get(self.__cfg_name, 'update_date'),
                                                          '%Y-%m-%d').date()
        except (configparser.Error, Exception) as e:
            raise Exception(e)

    def update(self, name=None, version=None, install_date=None, update_date=None):
        if name is not None:
            self.__config.set(self.__cfg_name, 'name', str(name))
            self.name = name
        if version is not None:
            self.__config.set(self.__cfg_name, 'version', str(version))
            self.version = version
        if install_date is not None:
            self.__config.set(self.__cfg_name, 'install_date', install_date.strftime('%Y-%m-%d'))
            self.install_date = install_date
        if update_date is not None:
            self.__config.set(self.__cfg_name, 'update_date', update_date.strftime('%Y-%m-%d'))
            self.update_date = update_date
        try:
            with open(self.__cfg_path, 'w') as f:
                self.__config.write(f)
        except configparser.Error:
            return False
        return True

    def read(self):
        try:
            self.__config.read(self.__cfg_path)
            self.name = self.__config.get(self.__cfg_name, 'name')
            self.version = self.__config.get(self.__cfg_name, 'version')
            self.install_date = datetime.datetime.strptime(self.__config.get(self.__cfg_name, 'install_date'),
                                                           '%Y-%m-%d').date()
            self.update_date = datetime.datetime.strptime(self.__config.get(self.__cfg_name, 'update_date'),
                                                          '%Y-%m-%d').date()
        except (configparser.Error, ValueError):
            return False
        return True
