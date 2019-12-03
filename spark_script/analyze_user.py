# coding=u8
import json
import operator
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf, col, lit, split, collect_list, struct
import pyspark.sql.functions as F
from pyspark.sql.types import *
import datetime
import sys


def map_line(line):

    ret_tuple = answer_id, answer_created, answer_updated, author_id, author_name, author_url_token, badge_num, can_comment,\
            comment_count, gender, insert_time, last_update_time, question_created, question_id,\
            reward_member_count, reward_total_money, voteup_count

    return ret_tuple

def analyze_url_token(data_list):
    url_token = ''
    old_url_token = ''
    url_token_max_date = {}
    sum_count = 0
    for val in data_list:
        url_token_max_date.update({val.author_url_token: val.max_answer_created})
        sum_count += val.sum_count


    if len(url_token_max_date.keys()) > 1:
        url_token = max(url_token_max_date.items(), key=operator.itemgetter(1))[0]
        old_url_token = min(url_token_max_date.items(), key=operator.itemgetter(1))[0]
        #print(old_url_token)
    else:
        url_token = list(url_token_max_date.keys())[0]

    max_date = max(url_token_max_date.items(), key=operator.itemgetter(1))[1]
    #result = '%s,%s' % (url_token, old_url_token)
    result = [url_token, old_url_token, str(sum_count), max_date.strip()]
    return result

def parse_max_answer_created():


    column_list = ['answer_id', 'answer_created', 'answer_updated', 'author_id', 'author_name', 'author_url_token', 'badge_num',\
            'can_comment', 'comment_count', 'gender', 'insert_time', 'last_update_time', 'question_created', 'question_id',\
            'reward_member_count', 'reward_total_money', 'voteup_count']

    df = spark.read.parquet("hdfs://device1/zhihu/basic_info/*")
    df = df.select(['author_id', 'author_url_token', 'answer_created'])
    df = df.withColumn('count',lit(1))
    df = df.groupby(['author_id', 'author_url_token']).agg({'answer_created': 'max', 'count': 'sum'})
    df = df.withColumnRenamed('max(answer_created)', 'max_answer_created').withColumnRenamed('sum(count)', 'sum_count')

    df = df.persist()

    my_udf = udf(analyze_url_token, ArrayType(StringType()))
    df = df.groupby(['author_id']).agg(collect_list(struct("max_answer_created", "author_url_token", "sum_count")).alias('struct')) \
    .withColumn('struct', my_udf('struct'))
    df = df.persist()  # 多用cache以减少重复计算
    df = df.withColumn('url_token', col('struct').getItem(0)).withColumn('old_url_token', col('struct').getItem(1)).withColumn('sum_count', col('struct').getItem(2)).withColumn('max_answer_created', col('struct').getItem(3))

    df_user = df.select(['author_id', 'url_token', 'old_url_token', 'sum_count', 'max_answer_created'])
    df_user.write.mode('overwrite').parquet('hdfs://device1/zhihu/users/max_answer_created')


def dump_active_user():
    df = spark.read.parquet("hdfs://device1/zhihu/users/max_answer_created")
    df = df.withColumn("count", df["sum_count"].cast(IntegerType()))
    df = df.select(['author_id', 'url_token', 'old_url_token', 'sum_count', 'max_answer_created'])
    df = df.filter("count >=5").filter("`max_answer_created` >= '2018-01-01'").filter("author_id != '0'").orderBy('count', ascending=False)
    print(df.show(50, False))

    url_token_list = df.select('url_token').collect()

    print(len(url_token_list))
    with open('active_url_token.txt', 'w') as f:
        for row in url_token_list:
            f.write(row.url_token + '\n')

    """
    df.createOrReplaceTempView("table1")
    df = spark.sql("select * from table1 where answer_created >= '2018-01-01'")
    df_group = df.groupby(['author_id']).agg(F.sum('count').alias('count'))
    df_group.cache()

    df_filter = df_group.filter("`count` >= 5").orderBy('count', ascending=False)

    df_filter.cache()
    print(df_filter.show(50, False))
    print(df_filter.count())
    """

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("zhihu_parse_basic_info")
    #conf.set("spark.driver.memory","30g")  # 要写对参数不然不会生效，之前写的是driver-memory ;注意：一定要在命令行中设置参数--driver-memory 不然无效!
    conf.set('spark.sql.warehouse.dir', 'hdfs://device1/user/dennis/spark/')
    conf.set('spark.executor.cores', '6')
    conf.set('spark.executor.instances', '2')


    sc = pyspark.SparkContext(conf=conf)
    spark = pyspark.sql.SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    #spark.sparkContext.setLogLevel('WARN')

    #parse_max_answer_created()
    dump_active_user()
