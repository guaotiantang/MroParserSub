
import multiprocessing
import sys
import time

from Config import MysqlInfo, FTPInfo
from ShareInfo import MysqlInfo as Minfo, FtpInfo as Finfo
from Sync import FtpScanProcess


if __name__ == '__main__':

    print(FTPInfo().check(), MysqlInfo().check())
    finfo = Finfo(FTPInfo())
    minfo = Minfo(MysqlInfo())
    minfo.db_name = "mroparse"
    minfo.tb_name = None
    manager = multiprocessing.Manager()
    manager_dict = manager.dict()
    manager_dict['status'] = True
    ftp_scan_process = FtpScanProcess(manager_dict, finfo, minfo, 10)
    ftp_scan_process.start()
    time.sleep(1)
    print("start")
    while True:
        comm = input("command:")
        if comm == "exit":
            ftp_scan_process.stop()
            ftp_scan_process.join()
            print("stoped")
            sys.exit()
        else:
            pass

