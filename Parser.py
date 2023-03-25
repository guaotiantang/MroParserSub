import io
import csv
from lxml import etree
import zipfile as mrozip
from typing import List, Dict, Optional

from Logs import ErrorLog
from ShareInfo import MysqlInfo


class MroPkg:
    def __init__(self, mysql_info: MysqlInfo, file_path: str = None):
        self.mysql_info = mysql_info
        self.file_path = file_path
        self.errlog = ErrorLog(self.mysql_info)

    def scan_xml_list(self, file_path: Optional[io.BytesIO] = None,
                      parent_path: Optional[List[str]] = None,
                      max_depth: Optional[int] = None) -> List[Dict[str, str]]:
        xml_list = []
        try:
            if file_path is None:
                if self.file_path is None:
                    return []
                file_path = io.BytesIO(open(self.file_path, 'rb').read())

            with mrozip.ZipFile(file_path) as zf:
                for name in zf.namelist():
                    try:
                        if name.endswith('.xml'):
                            path = parent_path if parent_path else []
                            xml_list.append(
                                {'main': self.file_path, 'path': '->'.join(map(str, path)), 'xml_file': name})
                        elif not name.endswith('/'):
                            sub_path = parent_path + [name] if parent_path else [name]
                            with io.BytesIO(zf.read(name)) as sub_file:
                                xml_list.extend(self.scan_xml_list(sub_file, sub_path, max_depth))
                    except Exception as e:
                        self.errlog.add_error(f"Error reading file {name}: {e}")

            if max_depth is not None and (not parent_path or len(parent_path) >= max_depth):
                return xml_list

            return xml_list
        except Exception as e:
            self.errlog.add_error(f"Error scanning XML list: {e}")
            return []

    def read_xml_data(self, xml_info: Dict[str, str]) -> Optional[bytes]:
        try:
            if 'path' not in xml_info or 'xml_file' not in xml_info:
                return None
            path_list = xml_info['path'].split('->') if xml_info['path'] else []
            main_path = xml_info.get('main', self.file_path)
            if main_path is None:
                return None
            file_obj = io.BytesIO(open(main_path, 'rb').read())
            for path in path_list:
                zf = mrozip.ZipFile(file_obj)
                sub_file = io.BytesIO(zf.read(path))
                file_obj = sub_file
            with mrozip.ZipFile(file_obj, 'r') as zf:
                return zf.read(xml_info['xml_file'])
        except Exception as e:
            self.errlog.add_error(f"An error occurred while reading XML data: {e}")
            return None


class XmlParse:
    def __init__(self, xmlio, mysql_info: MysqlInfo):
        self.xmlio = xmlio
        self.mysql_info = mysql_info
        self.errlog = ErrorLog(self.mysql_info)

    def parse(self):
        try:
            tree = etree.parse(self.xmlio)
            enb_id = tree.find('.//eNB').attrib['id']
            csvio = io.StringIO()
            csv_writer = csv.writer(csvio)

            headers_written = False

            for measurement in tree.findall('.//measurement'):
                smr_content = measurement.find('smr').text.strip()
                if not smr_content.startswith(
                        "MR.LteScEarfcn MR.LteScPci MR.LteScRSRP MR.LteScRSRQ "
                        "MR.LteScTadv MR.LteScPHR MR.LteScAOA MR.LteScSinrUL"):
                    continue

                smr_fields = smr_content.split()

                for obj in measurement.findall('object'):
                    object_id = obj.attrib['id']
                    MmeUeS1apId = obj.attrib['MmeUeS1apId']
                    MmeCode = obj.attrib['MmeCode']
                    MmeGroupId = obj.attrib['MmeGroupId']
                    TimeStamp = obj.attrib['TimeStamp']

                    for v in obj.findall('v'):
                        values = v.text.strip().split()
                        row_data = [enb_id, object_id, MmeUeS1apId, MmeCode, MmeGroupId, TimeStamp] + values

                        if not headers_written:
                            headers = ["enb_id", "object_id", "MmeUeS1apId", "MmeCode", "MmeGroupId",
                                       "TimeStamp"] + smr_fields
                            csv_writer.writerow(headers)
                            headers_written = True

                        csv_writer.writerow(row_data)

            csvio.seek(0)
            return io.BytesIO(csvio.read().encode())
        except Exception as e:
            self.errlog.add_error(e)
