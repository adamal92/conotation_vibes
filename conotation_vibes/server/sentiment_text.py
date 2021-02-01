from Data_Lake_2 import *
from data_types_and_structures import DataTypesHandler

import os


def main():

    # get data
    with open(fr'{os.getcwd()}\wiki.txt', "r", encoding='utf-8') as f:
        # processing
        DataTypesHandler.print_data_recursively(
            data=recognize(f, "israel"), print_dict=DataTypesHandler.PRINT_DICT
        )

        # data lake (save)
        from Hadoop.hdfs import HDFS_handler
        update_file_to_hdfs(hadoop_path=f"connotation", local_file_path=f.name)

        # data warehouse/mart (standardize)
        process_file(process_data_fn=lambda df: df, format="json", file="vehicles.json").show()

        # extract info (analyze)
        ml_example()

        # make accesible
        Constants.db.update()

        from Hadoop.hdfs import HDFS_handler
        # HDFS_handler.delete_file(filename=os.path.basename(f.name))

        # update_file_to_hdfs(hadoop_path=os.path.basename(f.name), local_file_path=f.name)
        # update_file_to_hdfs(filename=f.name,
        #                     file_path=os.path.dirname(os.path.abspath(f.name)))
        # from pathlib import Path
        # path = Path("/here/your/path/file.txt")
        # print(path.parent)


if __name__ == "__main__":
    main()
