import logging
import os
import time
from io import TextIOWrapper
from typing import Any

import pyspark
import requests
from pyspark.shell import spark
from pyspark.sql import DataFrame
from textblob import TextBlob

import nltk
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('brown')
from nltk.stem import LancasterStemmer


def recognize(file_string: TextIOWrapper, keyword: str) -> dict:
    """
        param file_string: A file that represent a data file
        param keyword: A string with a keyword to search
    """
    # Here goes the scraping result for articles .....
    feed = file_string.read()
    blob = TextBlob(feed)
    # print(blob.sentiment)
    # print(blob.tags)
    # print(blob.noun_phrases)
    # print(blob.sentiment.polarity)
    # print(blob.words)
    # print(blob.sentences)
    lst = LancasterStemmer()
    ret_dict = {}
    counter = 0
    ret_dict["connotation_list"] = []
    for sentence in blob.sentences:
        # print(sentence)
        for word in sentence.words:
            # print(lst.stem(word))
            if lst.stem(word) == lst.stem(keyword):
                counter += 1
                ret_dict["connotation_list"].append(
                    {
                        "polarity": sentence.sentiment.polarity,
                        "subjectivity": sentence.sentiment.subjectivity,
                        "sentence": sentence.correct()
                    }
                )
                # print(sentence.sentiment)
    ret_dict["times_found"] = counter
    ret_dict["total_connotation"] = {
        "polarity": blob.sentiment.polarity,
        "subjectivity": blob.sentiment.subjectivity
    }
    return ret_dict


def update_file_to_hdfs(hadoop_path: str, local_file_path: str):
    from Hadoop.hdfs import HDFS_handler
    HDFS_handler.start()

    time.sleep(2)

    # HDFS_handler.mkdir("user/hduser/webhdfs")
    # HDFS_handler.mkdir("user/hduser/webhdfs/v1")
    # HDFS_handler.mkdir("user/hduser/connotation")
    # time.sleep(2)

    # HDFS_handler.delete_file(filename=filename)
    # HDFS_handler.create_file(file_path=local_file_path)
    # os.system(fr'hdfs dfs -put {local_file_path} {hadoop_path}')
    from Hadoop.hdfs import HDFS_handler
    # HDFS_handler.create_file(file_path=local_file_path, hadoop_path='')
    # HDFS_handler.create_file(file_path=local_file_path, hadoop_path='connotation/')
    # time.sleep(2)

    connect_to_hdfs()

    # time.sleep(2)
    # HDFS_handler.list_files()
    # HDFS_handler.delete_file('first_sqlite_db.db')
    # HDFS_handler.delete_file('motorcycles.jl')
    # HDFS_handler.delete_file('motorcycles.json')
    # HDFS_handler.delete_file('quotes.jl')
    # HDFS_handler.delete_file('random.csv')
    # HDFS_handler.delete_file('vehicles.json')
    #
    # HDFS_handler.get_file(
    #     hdfs_file_path=rf'{HDFS_handler.HADOOP_USER}/city_json.json',
    #     local_path=r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\conotation_vibes\server\hdfs files'
    # )
    # HDFS_handler.delete_file('city_json.json')
    # HDFS_handler.get_file(
    #     hdfs_file_path=rf'{HDFS_handler.HADOOP_USER}/first_sqlite_db.db',
    #     local_path=r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\conotation_vibes\server\hdfs files'
    # )
    # HDFS_handler.get_file(
    #     hdfs_file_path=rf'{HDFS_handler.HADOOP_USER}/motorcycles.jl',
    #     local_path=r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\conotation_vibes\server\hdfs files'
    # )
    # HDFS_handler.get_file(
    #     hdfs_file_path=rf'{HDFS_handler.HADOOP_USER}/motorcycles.json',
    #     local_path=r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\conotation_vibes\server\hdfs files'
    # )
    # HDFS_handler.get_file(
    #     hdfs_file_path=rf'{HDFS_handler.HADOOP_USER}/quotes.jl',
    #     local_path=r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\conotation_vibes\server\hdfs files'
    # )
    # HDFS_handler.get_file(
    #     hdfs_file_path=rf'{HDFS_handler.HADOOP_USER}/random.csv',
    #     local_path=r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\conotation_vibes\server\hdfs files'
    # )
    # HDFS_handler.get_file(
    #     hdfs_file_path=rf'{HDFS_handler.HADOOP_USER}/vehicles.json',
    #     local_path=r'C:\Users\adam l\Desktop\python files\BigData\BD_projects\conotation_vibes\server\hdfs files'
    # )
    HDFS_handler.ls('user/hduser/connotation')
    HDFS_handler.list_files()

    HDFS_handler.stop()


def connect_to_hdfs():
    # https://stackoverflow.com/questions/48735869/error-failed-to-retrieve-data-from-webhdfs-v1-op-liststatus-server-error-wh/59112061#59112061
    # https://hadoop.apache.org/docs/r3.1.1/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#List_a_Directory

    # http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS
    # response = requests.get(url='http://localhost:9870/webhdfs/v1/user',
    #              params={"op": "LISTSTATUS"})
    from Hadoop.hdfs import HDFS_handler
    HDFS_handler.safemode_off()

    # response = requests.get(
    #     url='http://localhost:9870/webhdfs/v1/webhdfs/v1/wiki.txt',
    #     params={"op": "OPEN"}
    # )
    time.sleep(3)
    # hdfs_url = 'http://localhost:9870/webhdfs/v1/test/wiki.json'
    hdfs_url = 'http://localhost:9870/webhdfs/v1/test/wiki.json'
    response = requests.put(
        url=hdfs_url,
        params={
            "op": "CREATE", "overwrite": "true", 'user.name': 'livne'
        },
        data='{"test1": "test2", "test tast im testing": 2}'
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

    HDFS_handler.safemode_on()

    # from hdfs import InsecureClient
    #
    # # client = InsecureClient("http://localhost:9870/dfshealth.html#tab-overview")#, user='/user/hduser/webhdfs/v1/')
    # client = InsecureClient(url="http://localhost:9870/", user='livne')
    # client.list(hdfs_path='webhdfs/v1/webhdfs/v1/wiki.txt')
    # print(client.url)
    # Loading a file in memory.
    # with client.read('webhdfs/v1/wiki.txt', encoding='utf-8') as reader:
    #     features = reader.read()
    #     print(features)

    # http://localhost:9870/webhdfs/v1/webhdfs/v1/wiki.txt?op=OPEN
    # PUT http://laptop-i502rda8:9864/webhdfs/v1/webhdfs/v1/ffff.txt?op=CREATE
    # &namenoderpcaddress=localhost:9820&createflag=&createparent=true&overwrite=false

    # import cdata.hdfs as mod
    # conn = mod.connect("User=user@domain.com;")
    #
    # # Create cursor and iterate over results
    # cur = conn.cursor()
    # cur.execute("SELECT * FROM HDFSData")
    #
    # rs = cur.fetchall()
    #
    # for row in rs:
    #     print(row)


def process_file(process_data_fn, file: str, format: str) -> Any:
    st = time.time()

    from Hadoop.hdfs import HDFS_handler

    HDFS_handler.start()
    HDFS_handler.safemode_off()

    ret_processed: Any = pass_file_to_spark(
        file_path=f"{HDFS_handler.DEFAULT_CLUSTER_PATH}{HDFS_handler.HADOOP_USER}/{file}",
        process_fn=process_data_fn, format=format
    )

    HDFS_handler.safemode_on()
    HDFS_handler.stop()

    logging.debug(f"Spark Time: {time.time() - st} seconds ({(time.time() - st)//60} minutes)")
    return ret_processed


def pass_file_to_spark(process_fn, file_path: str, format: str, **kwargs) -> Any:
    """
    Passes a given file to spark and processes it with a given function
    :param process_fn :type function(data_frame: DataFrame) -> the function that processes the given file
    :param file_path :type str: the path of the file that should be processed
    :return :type dict:
    """
    from Hadoop.hdfs import HDFS_handler
    from py4j.protocol import Py4JJavaError
    try:
        df: DataFrame = spark.read.load(path=file_path, format=format, **kwargs)
        # df.show()
        return process_fn(df)
        # return process_fn(data_frame=df)

    except Py4JJavaError as e:
        logging.debug(type(e.java_exception))
        if "java.net.ConnectException" in e.java_exception.__str__():
            logging.error("HDFS cluster is down")
        else:
            HDFS_handler.stop()
            raise
    except:
        HDFS_handler.stop()
        raise


def ml_example(csv_path: str='C:/Users/adam l/Desktop/python files/BigData/BD_projects/Ruby_corona_charts/excel_sheets/boston_housing.csv'):
    # load data
    data: DataFrame = spark.read.csv(csv_path, header=True, inferSchema=True)
    # create features vector
    feature_columns: list = data.columns[:-1]  # here we omit the final column
    from pyspark.ml.feature import VectorAssembler
    assembler: VectorAssembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data_2: DataFrame = assembler.transform(data)
    # train/test split
    test: DataFrame
    train: DataFrame
    train, test = data_2.randomSplit([0.7, 0.3])
    # define the model
    from pyspark.ml.regression import LinearRegression
    algo: LinearRegression = LinearRegression(featuresCol="features", labelCol="medv")
    # train the model
    model: pyspark.ml.regression.LinearRegressionModel = algo.fit(train)
    # evaluation
    evaluation_summary: pyspark.ml.regression.LinearRegressionSummary = model.evaluate(test)
    print(
        evaluation_summary.meanAbsoluteError,
        evaluation_summary.rootMeanSquaredError,
        evaluation_summary.r2
    )
    # predicting values
    predictions: DataFrame = model.transform(test)
    predictions.select(
        predictions.columns[13:]
    ).show()  # here I am filtering out some columns just for the figure to fit

