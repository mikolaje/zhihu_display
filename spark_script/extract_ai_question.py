# coding=u8
import json
import re
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf, col, lit, lower
import pyspark.sql.functions as F
from pyspark.sql.types import *
import datetime
import sys
try:
    reload(sys)
    sys.setdefaultencoding('u8')
except:
    pass


def map_line(line):

    ret_tuple = answer_id, answer_created, answer_updated, author_id, author_name, author_url_token, badge_num, can_comment,\
            comment_count, gender, insert_time, last_update_time, question_created, question_id,\
            reward_member_count, reward_total_money, voteup_count

    return ret_tuple

def is_ai_title(title):
    is_ai = False
    match1 = re.search(r'\bai\b', title.lower())
    if match1:
        is_ai = True
    if '人工智能' in title:
        is_ai = True
    if '机器学习' in title:
        is_ai = True
    if '神经网络' in title:
        is_ai = True
    if '自动驾驶' in title:
        is_ai = True

    return is_ai


def parse_question():

    column_list = ['answer_id', 'answer_content', 'answer_created', 'answer_updated', 'author_id', 'author_name', 'author_url_token', 'badge_num',\
            'can_comment', 'comment_count', 'gender', 'insert_time', 'question_created', 'question_title', 'question_id',\
            'reward_member_count', 'reward_total_money', 'voteup_count']

    df = spark.read.parquet("hdfs://device1/zhihu/basic_info/*")
    #df = spark.read.option("mergeSchema", False).parquet("hdfs://device1/zhihu/basic_info/2017*")

    df = df.select(column_list)
    check_ai = udf(is_ai_title, BooleanType())
    df = df.withColumn('count', lit(1))
    df = df.withColumn('is_ai', check_ai('question_title'))
    df = df.filter(df.is_ai == True)
    df = df.withColumn('answer_created', df.answer_created.cast(DateType()))
    df = df.withColumn('answer_updated', df.answer_updated.cast(DateType()))
    df = df.withColumn('insert_time', df.insert_time.cast(DateType()))
    df = df.withColumn('question_created', df.question_created.cast(DateType()))
    print(df.printSchema())
    print(df.show())
    #df = df.repartition(1)
    #df.write.mode('overwrite').csv('hdfs://device1/zhihu/ai_question')
    """
    df.write.format('jdbc').options(
	  url='jdbc:mysql://35.220.239.102/zhihu_display',
	  driver='com.mysql.jdbc.Driver',
	  dbtable='ai_questions',
	  user='ramsey',
	  password='AaronRamsey10!').mode('overwrite').save()
    """
    df_pandas = df.toPandas()
    df_pandas.to_excel('output.xlsx')
    #print(df_pandas)


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("zhihu_parse_basic_info")
    #conf.set("spark.driver.memory","30g")  # 要写对参数不然不会生效，之前写的是driver-memory ;注意：一定要在命令行中设置参数--driver-memory 不然无效!
    conf.set('spark.sql.warehouse.dir', 'hdfs://device1/user/dennis/spark/')
    conf.set('spark.executor.cores', '6')
    conf.set('spark.executor.instances', '6')


    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('INFO')
    parse_question()

