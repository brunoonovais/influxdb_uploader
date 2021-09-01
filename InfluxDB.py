import logging
import threading
import time
from typing import Dict, Any, List
from requests import request, Response

class TooBig(Exception):
    pass

class InfluxDBUploader:

    def __init__(self, *args, **kwargs) -> None:
        # values set by instantiation
        self.ip: str = kwargs['ip']
        self.port: str = kwargs['port']
        self.log: Logger = logging.getLogger('influxdb')
        self.database: str = kwargs['database']
        # end of values set by instantiation

        self.url: str = f"http://{self.ip}:{self.port}"
        self.api_url: str = f"{self.url}/api/v2/write?precision=ms&bucket={self.database}"
        self.log.debug(self.url)

        self.log.debug("Started InfluxDB Uploader")

        if "username" in kwargs:
            base_64_auth: str = f'{kwargs["username"]}:{kwargs["password"]}'
            base_64_auth: bytes = base64.b64encode(base_64_auth.encode())
            base_64_auth: str = base_64_auth.decode()
            self.headers = {
                'Content-Type': 'text/plain',
                'Authorization': f'Basic {base_64_auth}'
            }
        else:
            self.headers = {
                'Content-Type': 'text/plain'
            }

    def _create_line_protocol(self, data) -> str:
        '''
        This function receives a dictionary and converts to influxdb line protocol format

        :param data:
        :return:
        '''

        # for every upload, we need the following format in a single line: "measurement tags fields timestamp"
        line_measurement = data['measurement_name']
        line_tags = []
        line_fields = []
        line_timestamp = data['timestamp']

        for key, value in data.items():
            if key in ["measurement_name", "timestamp"]:
                continue
            elif isinstance(value, str):
                line_tags.append(f"{key}={value}")
            else:
                line_fields.append(f"{key}={value}")

        line = f'{line_measurement},{",".join(line_tags)} {",".join(line_fields)} {line_timestamp}'

        return line

    def _send(self, lines) -> bool:
        '''
        This function will grab the lineprotocol data and send to influxdb.

        :param data:
        :return:
        '''

        try:
            print(f'uploading {len(lines)} lines to {self.api_url}')
            print(f'headers={self.headers}')
            response = request("POST", self.api_url, headers=self.headers, data="\n".join(lines))
            print(response)
        except Exception as e:
            print(f'Exception. \n{e}')
        else:
            return True

    def upload(self, data) -> bool:
        '''
        This function receives a dictionary to upload to influxdb.

        :param data:
        :return:
        '''

        line = self._create_line_protocol(data)

        if self._send([line]):
            return True
        else:
            return False

    def upload_bulk(self, bulk_list) -> bool:
        '''
        This function receives a list of dictionaries to upload to influxdb.

        :param bulk_list:
        :return:
        '''

        # for each dictionary, we will build a line protocol entry and append to 'lines'
        lines=[]
        [lines.append(self._create_line_protocol(item)) for item in bulk_list]

        if self._send(lines):
            return True
        else:
            return False

class InfluxDBUploaderThread(threading.Thread):

    def __init__(self, *args, **kwargs) -> None:
        threading.Thread.__init__(self)
        # values set by instantiation
        self.ip: str = kwargs['ip']
        self.port: str = kwargs['port']
        self.log: Logger = logging.getLogger('influxdb')
        self.database: str = kwargs['database']
        self.batch_size = kwargs['batch_size']
        self.sleep = kwargs['sleep']
        # end of values set by instantiation

        self.bulk_list = []
        self.url: str = f"http://{self.ip}:{self.port}"
        self.api_url: str = f"{self.url}/api/v2/write?precision=ms&bucket={self.database}"
        self.log.debug(self.url)
        self.log.debug("Started InfluxDB Uploader")

        if "username" in kwargs:
            base_64_auth: str = f'{kwargs["username"]}:{kwargs["password"]}'
            base_64_auth: bytes = base64.b64encode(base_64_auth.encode())
            base_64_auth: str = base_64_auth.decode()
            self.headers = {
                'Content-Type': 'text/plain',
                'Authorization': f'Basic {base_64_auth}'
            }
        else:
            self.headers = {
                'Content-Type': 'text/plain'
            }

    def _create_line_protocol(self, data) -> str:
        '''
        This function receives a dictionary and converts to influxdb line protocol format

        :param data:
        :return:
        '''

        # for every upload, we need the following format in a single line: "measurement tags fields timestamp"
        line_measurement = data['measurement_name']
        line_tags = []
        line_fields = []
        line_timestamp = data['timestamp']

        for key, value in data.items():
            if key in ["measurement_name", "timestamp"]:
                continue
            elif isinstance(value, str):
                #print(f'key={key} & value = {value} are string')
                line_tags.append(f"{key}={value}")
            else:
                #print(f'key={key} & value = {value} are NOT string')
                line_fields.append(f"{key}={value}")

        line = f'{line_measurement},{",".join(line_tags)} {",".join(line_fields)} {line_timestamp}'
        #print(f'line = {line}')

        return line

    def _send(self, lines) -> bool:
        '''
        This function will grab the lineprotocol data and send to influxdb.

        :param data:
        :return:
        '''

        try:
            print(f'uploading {len(lines)} lines to {self.api_url}')
            #print(f'headers={self.headers}')
            #print(f'lines=\n{lines}')
            response = request("POST", self.api_url, headers=self.headers, data="\n".join(lines))
            #print(response)
        except Exception as e:
            print(f'Exception. \n{e}')
            return False
        else:
            return True

    def run(self) -> bool:
        '''
        This function keeps receiving appended items to bulk_list and invokes the upload once we reach the batch size

        :param bulk_list:
        :return:
        '''

        # for each dictionary, we will build a line protocol entry and append to 'lines'
        while True:
            self.log.debug(f'size of bulk_list = {self.bulk_list}')

            # bulk_list needs to be equal to batch size and smaller than 5000 for influxdb
            bulk_list_size = len(self.bulk_list)
            if bulk_list_size >= self.batch_size and bulk_list_size < 5000:
                lines=[]
                [lines.append(self._create_line_protocol(item)) for item in self.bulk_list]
                
                # if upload is successful, we clear the bulk_list
                if self._send(lines):
                    self.bulk_list.clear()
                else:
                    self.log.error(f'failed to upload to influxdb')
            elif bulk_list_size >= 5000:
                raise TooBig("bulk_list has grown beyond acceptable limit of 5000. Exiting...")
            
            self.log.info(f'sleeping for {self.sleep}')
            time.sleep(self.sleep)
