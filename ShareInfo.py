from Config import *


class MysqlInfo:
    def __init__(self, info=None):
        info = info or MysqlInfo()
        self.host, self.port, self.user, self.passwd, self.db_name, self.tb_name \
            = info.host, info.port, info.user, info.passwd, info.db_name, info.tb_name


class FtpInfo:
    def __init__(self, info=None):
        info = info or FTPInfo()
        self.ftp_name, self.host, self.port, self.user, self.passwd, self.sync_path, \
            self.down_path, self.scan_filter = info.ftp_name, info.host, info.port, info.user, \
            info.passwd, info.sync_path, info.down_path, info.scan_filter


class PerfInfo:
    def __init__(self, info=None):
        info = info or PerfInfo()
        self.processes, self.threads = info.processes, info.threads
