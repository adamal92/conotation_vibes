from textblob import TextBlob
import os

def recognize(file_string: str, keyword: str):
    """
        param file_string: A string that represent a data from file
        param keyword: A string with a keyword to search
    """
    # Here goes the scraping result for articles .....
    blob = TextBlob(file_string)
    print(blob.sentiment)
   

def main():
    """
        Main function of the server.
        Includes the calls for conotation recognition functions and calls the data base queries
    """
    with open(fr'{os.getcwd()}\wiki.txt', "r") as file:
        recognize(file.read().lower(), "israel")
    # feed = "the food at Ruby's place is awful"
    # blob = TextBlob(feed)
    # print(blob.sentiment)

if __name__ == "__main__":
    main()