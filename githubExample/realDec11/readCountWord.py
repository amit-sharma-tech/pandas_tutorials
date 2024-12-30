from pyspark import SparkContext
import logging

def wordCount(file_path):
    #step 1 create sparkcontext 
    sc = SparkContext(appName="WordCont")

    try:
        #step 2: read text file and split it in words
        lines = sc.textFile(file_path)
        words = lines.flatMap(lambda x: x.split(" "))

        #step 3 count the occurances of each word

        word_count = words.countByValue()

        #step4 print the word counts

        for word,count in word_count.items():
            print(f"{word} : {count}")
        # comment: 
    except Exception as e:
        logging.error('error while generate exception :{}'.format())
        # raise e
    finally:
        sc.stop()
    # end try

filepath = "../sources/text.txt"
wordCount(filepath)