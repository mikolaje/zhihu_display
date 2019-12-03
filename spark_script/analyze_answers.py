# coding=u8
from utils import toPandas
import json
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


def split_date(_datetime):
    _datetime = datetime.datetime.strptime(_datetime, '%Y-%m-%dT%H:%M:%S')

    ret = _datetime.year, _datetime.month, _datetime.day, \
            _datetime.hour, _datetime.minute, _datetime.second, _datetime.weekday(), str(_datetime.date())

    return ret


def parse_data():

    column_list = ['answer_id', 'answer_created', 'answer_updated', 'author_id', 'author_name', 'author_url_token', 'badge_num',\
            'can_comment', 'comment_count', 'gender', 'insert_time', 'question_created', 'question_title', 'question_id',\
            'reward_member_count', 'reward_total_money', 'voteup_count']

    df = spark.read.parquet("hdfs://device1/zhihu/basic_info/*")
    #df = spark.read.json("hdfs://device1/zhihu/basic_info2/*")
    print(df.show())
    df = df.select(['answer_id', 'answer_created'])
    df = df.withColumn('count',lit(1))
    my_udf = udf(split_date, ArrayType(StringType()))
    df = df.withColumn('new_date', my_udf('answer_created'))
    df = df.persist()
    df = df.withColumn('year', col('new_date').getItem(0)).withColumn('month', col('new_date').getItem(1)) \
            .withColumn('day', col('new_date').getItem(2)).withColumn('hour', col('new_date').getItem(3)) \
            .withColumn('minute', col('new_date').getItem(4)).withColumn('second', col('new_date').getItem(5)) \
            .withColumn('weekday', col('new_date').getItem(6)).withColumn('date', col('new_date').getItem(7))

    df = df.select(['answer_id', 'year', 'month', 'day', 'hour', 'minute', 'second', 'weekday', 'date', 'count'])
    df = df.groupby(['year', 'month', 'day', 'hour', 'date', 'weekday']).agg(F.sum('count').alias('count'))

    df.write.mode('overwrite').parquet('hdfs://device1/zhihu/answers_datetime_scatter/')

def parse_data2():
    df = spark.read.parquet("hdfs://device1/zhihu/basic_info/*")
    #df = spark.read.json("hdfs://device1/zhihu/basic_info2/*")
    df = df.select(['answer_id', 'gender', 'author_id', 'author_name', 'answer_created'])
    df = df.withColumn('count',lit(1))

    my_udf = udf(split_date, ArrayType(StringType()))
    df = df.withColumn('new_date', my_udf('answer_created'))

    df = df.withColumn('year', col('new_date').getItem(0)).withColumn('month', col('new_date').getItem(1)) \
            .withColumn('day', col('new_date').getItem(2)).withColumn('hour', col('new_date').getItem(3)) \
            .withColumn('minute', col('new_date').getItem(4)).withColumn('second', col('new_date').getItem(5)) \
            .withColumn('weekday', col('new_date').getItem(6)).withColumn('date', col('new_date').getItem(7))

    df = df.select(['author_id', 'gender', 'author_name', 'year', 'month', 'day', 'hour', 'minute', 'second', 'weekday', 'date', 'count'])
    df = df.persist()
    df = df.groupby(['author_id', 'gender', 'author_name', 'year', 'month', 'day', 'hour', 'date', 'weekday']).agg(F.sum('count').alias('count'))
    df.write.mode('overwrite').json('hdfs://device1/zhihu/gender_name/')
    #df_pandas.to_csv('./gender_name.csv')

def parse_data3():
    df = spark.read.parquet("hdfs://device1/zhihu/basic_info/*")
    #df = spark.read.json("hdfs://device1/zhihu/basic_info2/*")
    df = df.select(['question_id', 'question_created', 'gender', 'author_id', 'answer_id', 'answer_created'])
    df = df.withColumn('count',lit(1))
    df_group = df.groupby(['question_id']).agg(F.count('answer_id').alias('count'))
    df_group = df_group.cache()

    df_group = df_group.orderBy('count', ascending=False)
    print(df_group.show())


def analyse():
    df = spark.read.parquet('hdfs://device1/zhihu/answers_datetime_scatter/')
    df_pandas = df.toPandas()
    df_pandas.to_csv('./datetime_distribution_answers.csv')


def analyse2():
    df = spark.read.json('hdfs://device1/zhihu/gender_name/')
    df = df.select(['author_id', 'gender', 'year', 'month', 'day', 'hour', 'weekday', 'date', 'count'])
    df = df.withColumn('count',lit(1))
    print(df.show())

    df_group = df.groupby(['gender', 'hour', 'weekday']).agg(F.sum('count').alias('count'))
    print(df_group.show())
    df_pandas = df_group.toPandas()

    #df_pandas = toPandas(df, 10)
    df_pandas.to_csv('./gender_hour_count.csv')


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("zhihu_parse_basic_info")
    #conf.set("spark.driver.memory","30g")  # 要写对参数不然不会生效，之前写的是driver-memory ;注意：一定要在命令行中设置参数--driver-memory 不然无效!
    conf.set('spark.sql.warehouse.dir', 'hdfs://device1/user/dennis/spark/')
    conf.set('spark.executor.cores', '6')
    conf.set('spark.executor.instances', '6')
    conf.set('spark.driver.maxResultSize', '11g')
    conf.set('spark.network.timeout', '5800s')
    conf.set("spark.executor.heartbeatInterval","4800s")



    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    #parse_data()
    parse_data2()
    #parse_data3()
    #analyse()
    #analyse2()

