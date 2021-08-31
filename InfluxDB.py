import logging
from typing import Dict, Any, List
from requests import request, Response


class InfluxDBUploader:

    def __init__(self, *args, **kwargs) -> None:
        self.ip: str = kwargs['ip']
        self.port: str = kwargs['port']
        self.log: Logger = logging.getLogger('influxdb')
        self.database: str = kwargs['database']

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


    def upload(self, data) -> bool:
        '''
        This function receives a dictionary to upload to influxdb.

        :param data:
        :return:
        '''

        line = self._create_line_protocol(data)

        try:
            print(f'uploading to {self.api_url}')
            print(f'headers={self.headers}')
            response = request("POST", self.api_url, headers=self.headers, data=line)
            print(response)
        except Exception as e:
            print(f'Exception. \n{e}')
            return False
        else:
            return True

    def upload_bulk(self, bulk_list) -> bool:
        '''
        This function receives a list of dictionaries to upload to influxdb.

        :param bulk_list:
        :return:
        '''

        # for each dictionary, we will build a line protocol entry and append to 'lines'
        lines=[]
        [lines.append(self._create_line_protocol(item)) for item in bulk_list]

        try:
            print(f'uploading {len(lines)} lines to {self.api_url}')
            print(f'headers={self.headers}')
            response = request("POST", self.api_url, headers=self.headers, data="\n".join(lines))
            print(response)
        except Exception as e:
            print(f'Exception. \n{e}')

        return True
