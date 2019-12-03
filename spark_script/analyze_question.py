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
from setting import MYSQL_PASSWORD

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




def analyse_question():
    df = spark.read.csv('hdfs://device1/zhihu/question_count')
    print(df.show())


def extract_question_title():
    """提取最新的question title 保存到MySQL中。"""
    df = spark.read.parquet("hdfs://device1/zhihu/basic_info/*")
    df = df.select(['question_id', 'question_title', 'answer_created', 'question_created'])

    df = df.groupby(['question_id', 'question_title', 'question_created']).agg(F.max('answer_created').alias('latest_answer_time'))
    df.write.format('jdbc').options(
            url='jdbc:mysql://localhost/zhihu?characterEncoding=utf8',
            driver='com.mysql.jdbc.Driver',
            dbtable='question_title',
            user='root',
            password=MYSQL_PASSWORD) \
            .option("encoding", "UTF-8").mode('overwrite').save()

    print(df.show())


def analyze_question_answers_count():

    column_list = ['answer_id', 'answer_created', 'answer_updated', 'author_id', 'author_name', 'author_url_token', 'badge_num',\
            'can_comment', 'comment_count', 'gender', 'insert_time', 'question_created', 'question_title', 'question_id',\
            'reward_member_count', 'reward_total_money', 'voteup_count']

    df = spark.read.parquet("hdfs://device1/zhihu/basic_info/*")
    #df = spark.read.json("hdfs://device1/zhihu/basic_info2/*")
    df = df.select(['question_id', 'question_created'])
    df = df.withColumn('count',lit(1))
    df.createOrReplaceTempView("table1")
    #df = spark.sql("select * from table1 where question_created >= '2018-01-01'")
    df_group = df.groupby('question_id').agg(F.sum('count').alias('count'))
    df_group.cache()

    df_filter = df_group.filter("`count` >= 30").orderBy('count', ascending=False)

    df_filter.cache()
    question_list = df_filter.select('question_id').collect()
    with open('active_questions.txt', 'w') as f:
        for row in question_list:
            f.write(str(row.question_id) + '\n')

    print(df_filter.show(50))
    # print(df_filter.count())


def is_code_question(question_title):
    keywords = ['java', 'python', 'php', 'scala', 'c#', 'c++', 'html', 'c语言', 'node', 'go']


def is_city_question(question_title):
    cities = ['北京', '上海', '广州', '深圳', '杭州', '天津', '厦门', '成都', '西安', '重庆', '香港']
    is_city = False

    for city in cities:
        if city in question_title:
            is_city = True

    return is_city


def parse_question():

    column_list = ['answer_id', 'answer_created', 'answer_updated', 'author_id', 'author_name', 'author_url_token', 'badge_num',\
            'can_comment', 'comment_count', 'gender', 'insert_time', 'question_created', 'question_title', 'question_id',\
            'reward_member_count', 'reward_total_money', 'voteup_count']

    df = spark.read.json("hdfs://device1/zhihu/basic_info2/*")
    #df = spark.read.option("mergeSchema", False).parquet("hdfs://device1/zhihu/basic_info/2017*")

    df = df.select(['gender', 'question_id', 'question_created', 'question_title'])

    df = df.withColumn('count',lit(1))
    df = df.groupby(['question_id', 'gender', 'question_created', 'question_title']).agg(F.sum('count').alias('answer_count'))

    df.write.mode('overwrite').csv('hdfs://device1/zhihu/question_count')

def check_program_lang(title):
    ret = [0, 0, 0, 0]  #java,python,javascript,php
    title = title.lower()
    if re.search(r'\bjava\b', title):
        ret[0] = 1
    if 'python' in title:
        ret[1] = 1
    if 'javascript' in title or re.search(r'\bjs\b', title):
        ret[2] = 1
    if 'php' in title:
        ret[3] = 1
    if sum(ret) > 0:
        ret.append(1)
    else:
        ret.append(0)

    return ret

def analyze_program_question():
    df = spark.read.json('hdfs://device1/zhihu/question_count')

    schema = StructType([
        StructField("java", IntegerType(), False),
        StructField("python", IntegerType(), False),
        StructField("js", IntegerType(), False),
        StructField("php", IntegerType(), False),
        StructField("is_lang", IntegerType(), False)
    ])

    check_lang_udf = udf(check_program_lang, schema)
    df = df.withColumn('lang', check_lang_udf('question_title'))
    df = df.select(['question_id', 'question_created', 'question_title', 'answer_count', 'lang.*'])
    df = df.filter(df.is_lang > 0)
    df = df.cache()
    print(df.count())
    df_pandas = df.toPandas()
    print(df_pandas)
    df_pandas.to_csv('./question_lang.csv')


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
    #parse_question()
    #analyse_question()
    #analyze_program_question()
    #analyze_question_answers_count()
    extract_question_title()
