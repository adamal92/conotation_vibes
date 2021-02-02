# data source -> hdfs
import time
from typing import Any

import requests
from serverLib import *
# from BD_projects.conotation_vibes.server_3.serverLib import *


def update_file_to_hdfs(hadoop_path: str, data: Any):  # local_file_path: str,
    from Hadoop.hdfs import HDFS_handler
    HDFS_handler.start()

    time.sleep(7)

    from Hadoop.hdfs import HDFS_handler

    connect_to_hdfs(hdfs_url=fr'http://localhost:9870/webhdfs/v1/{hadoop_path}', data=data)

    # HDFS_handler.ls('user/hduser/connotation')
    # HDFS_handler.list_files()

    HDFS_handler.stop()


def connect_to_hdfs(hdfs_url: str, data: Any):
    # https://stackoverflow.com/questions/48735869/error-failed-to-retrieve-data-from-webhdfs-v1-op-liststatus-server-error-wh/59112061#59112061
    # https://hadoop.apache.org/docs/r3.1.1/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#List_a_Directory

    # http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS
    # response = requests.get(url='http://localhost:9870/webhdfs/v1/user',
    #              params={"op": "LISTSTATUS"})
    from Hadoop.hdfs import HDFS_handler
    HDFS_handler.safemode_off()

    time.sleep(20)
    # hdfs_url = 'http://localhost:9870/webhdfs/v1/test/wiki.json'
    # hdfs_url = 'http://localhost:9870/webhdfs/v1/test/wiki.json'
    response = requests.put(
        url=hdfs_url,
        params={
            "op": "CREATE", "overwrite": "true", 'user.name': 'livne'
        },
        data=data
        # data='{"test1": "test2", "test tast im testing": 2}'
        # data = {"test1": "test2", "test tast im testing": 2}
    )
    try:
        print(response.json())
        from data_types_and_structures import DataTypesHandler
        DataTypesHandler.print_data_recursively(
            data=response.json(), print_dict=DataTypesHandler.PRINT_DICT
        )
    except:
        print(response, response.url)

    time.sleep(2)

    response = requests.get(
        url=hdfs_url,
        params={"op": "OPEN"}
    )
    try:
        print(response.json())
        from data_types_and_structures import DataTypesHandler
        DataTypesHandler.print_data_recursively(
            data=response.json(), print_dict=DataTypesHandler.PRINT_DICT
        )
    except:
        print(response, response.url)
        print(response.text)

    # hdfs dfs -get /connotation/testspeech.mp4 'C:\Users\adam l\Desktop\test'

    HDFS_handler.safemode_on()


def main():
    # TODO: add params support

    # Extract
    run_py_file(filename='download_youtube')
    # Transform
    run_py_file(filename='mp4_to_speech_text')
    # Load
    with open(rf'testspeech.mp4', 'rb') as mp4:
        # print(mp4.read())
        update_file_to_hdfs(hadoop_path='connotation/testspeech.mp4', data=mp4.read())


if __name__ == '__main__':
    main()
