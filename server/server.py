from textblob import TextBlob
import os


"""
This file contains the server of the program.
"""


def recognize(file_string, keyword: str):
    """
        param file_string: A file that represent a data file
        param keyword: A string with a keyword to search
    """
    # Here goes the scraping result for articles .....
    feed = file_string.read()
    blob = TextBlob(feed)
    print(blob.sentiment)


def main():
    """
        Main function of the server.
        Includes the calls for conotation recognition functions and calls the data base queries
    """
    with open(fr'{os.getcwd()}\wiki.txt', "r", encoding='utf-8') as f:
        recognize(f, "israel")
    # feed = "the food at Ruby's place is awful"
    # blob = TextBlob(feed)
    # print(blob.sentiment)


if __name__ == "__main__":
    main()
