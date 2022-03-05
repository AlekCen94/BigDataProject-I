from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

#Method for initialize spark.

MALDATA = os.environ['MALWER_DATA']
    
def init():
    spark = SparkSession.builder.appName('AndroidMalware"').master("spark://spark-master:7077").getOrCreate()

    return spark

#Method for parsing data from file.
#@param: inputhPath Path to the file.
#@param: spark SparkSession.


def parsingData(inputPath, spark):
    df = spark.read.options(header='True').csv(inputPath)
    df2 = df.withColumn(" Timestamp", to_timestamp(col(" Timestamp"), 'dd/MM/yyyy HH:mm:ss'))
    return df2

#Method for calculate sum, max, min, mean and stdDev of FlowDuration on Destination address and port.
#@param: address Destination address.
#@param:h Hour
#@param:m Minutes
#@param:s Seconds
#@param: df Dataframe.


def FlowDurationOnDestination(addres, h, m, s, df):
    colu = " Flow Duration"
    timeFormat = 'dd/MM/yyyy HH:mm:ss'
    df.filter((col(" Destination IP") == addres)\
    & (second(col(" Timestamp"))>=s)\
    & (minute(col(" Timestamp"))>=m)\
    & (hour(col(" Timestamp"))>=h))\
    .select(sum(col(colu)), max(col(colu)), min(col(colu)), mean(col(colu)), stddev(col(colu))).show()


#Method for count Packets on Destination address and port.
#@param: address Destination address.
#@param:h Hour
#@param:m Minutes
#@param:s Seconds
#@param: df Dataframe.


def countPacketsOnDestination(addres, h, m, s, df):
    print("CountPacketsOnDestination:",\
    df.filter((col(" Destination IP") == addres)\
    & (second(col(" Timestamp"))>=s)\
    & (minute(col(" Timestamp"))>=m)\
    & (hour(col(" Timestamp"))>=h)).count())


#Method for calculate sum, max, min, mean and stdDev of FwdPackets on Destination address and port.
#@param: address Destination address.
#@param:h Hour
#@param:m Minutes
#@param:s Seconds
#@param: df Dataframe.


def FwdPacketsOnDestination(addres, h, m, s, df):
    colu = " Total Fwd Packets"
    timeFormat = 'dd/MM/yyyy HH:mm:ss'
    df.filter((col(" Destination IP") == addres)\
    & (second(col(" Timestamp"))>=s)\
    & (minute(col(" Timestamp"))>=m)\
    & (hour(col(" Timestamp"))>=h))\
    .select(sum(col(colu)), max(col(colu)), min(col(colu)), mean(col(colu)), stddev(col(colu))).show()


#Method for calculate sum, max, min, mean and stdDev of BackWardPackets on Destination address and port.
#@param: address Destination address.
#@param:h Hour
#@param:m Minutes
#@param:s Seconds
#@param: df Dataframe.


def BackWardPacketsOnDestination(addres, h, m, s, df):
    colu = " Total Backward Packets"
    timeFormat = 'dd/MM/yyyy HH:mm:ss'
    df.filter((col(" Destination IP") == addres)\
    & (second(col(" Timestamp"))>=s)\
    & (minute(col(" Timestamp"))>=m)\
    & (hour(col(" Timestamp"))>=h))\
    .select(sum(col(colu)), max(col(colu)), min(col(colu)), mean(col(colu)), stddev(col(colu))).show()


#Method for calculate sum, max, min, mean and stdDev of LengthOfFwdPackets on Destination address and port.
#@param: address Destination address.
#@param:h Hour
#@param:m Minutes
#@param:s Seconds
#@param: df Dataframe.


def LengthOfFwdPacketsOnDestination(addres, h, m, s, df):
    colu = "Total Length of Fwd Packets"
    timeFormat = 'dd/MM/yyyy HH:mm:ss'
    df.filter((col(" Destination IP") == addres)\
    & (second(col(" Timestamp"))>=s)\
    & (minute(col(" Timestamp"))>=m)\
    & (hour(col(" Timestamp"))>=h))\
    .select(sum(col(colu)), max(col(colu)), min(col(colu)), mean(col(colu)), stddev(col(colu))).show()

#Method for calculate sum, max, min, mean and stdDev of LengthOfBackWardPackets on Destination address and port.
#@param: address Destination address.
#@param:h Hour
#@param:m Minutes
#@param:s Seconds
#@param: df Dataframe.


def LengthOfBackWardPacketsOnDestination(addres, h, m, s, df):
    colu = " Total Length of Bwd Packets"
    timeFormat = 'dd/MM/yyyy HH:mm:ss'
    df.filter((col(" Destination IP") == addres)\
    & (second(col(" Timestamp"))>=s)\
    & (minute(col(" Timestamp"))>=m)\
    & (hour(col(" Timestamp"))>=h))\
    .select(sum(col(colu)), max(col(colu)), min(col(colu)), mean(col(colu)), stddev(col(colu))).show()


if __name__ == "__main__":

    sc = init()

    data = parsingData(MALDATA, sc)
    FwdPacketsOnDestination("10.42.0.211", 0, 5, 20,data)

    BackWardPacketsOnDestination("10.42.0.211", 0, 5, 20,data)

    FlowDurationOnDestination("10.42.0.211", 0, 5, 20,data)

    LengthOfFwdPacketsOnDestination("10.42.0.211", 0, 5, 20,data)

    LengthOfBackWardPacketsOnDestination("10.42.0.211", 0, 5, 20,data)

    #print(data.filter((col(" Destination IP") == "10.42.0.211")).collect())
    sc.stop()

