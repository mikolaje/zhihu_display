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
reload(sys)
sys.setdefaultencoding('u8')



def main():
    print(spark.sparkContext.getConf().getAll())
    spark.sparkContext.setLogLevel('WARN')

    column_list = ['answer_id', 'answer_created', 'answer_updated', 'author_id', 'author_name', 'author_url_token', 'badge_num',\
            'can_comment', 'comment_count', 'gender', 'insert_time', 'last_update_time', 'question_created', 'question_id',\
            'reward_member_count', 'reward_total_money', 'voteup_count']

    df = spark.read.parquet("hdfs://device1/zhihu/users/max_answer_created")
    df.createOrReplaceTempView("table1")

    df = spark.sql("select * from table1 where max_answer_created >= '2018-01-01'")
    """
    df = df.select(['author_id', 'author_url_token', 'answer_created'])
    df = df.withColumn('count',lit(1))
    df = df.groupby(['author_id', 'author_url_token']).agg({'answer_created': 'max', 'count': 'sum'})
    df = df.withColumnRenamed('max(answer_created)', 'max_answer_created').withColumnRenamed('sum(count)', 'sum_count')

    df = df.cache()

    my_udf = udf(analyze_url_token, ArrayType(StringType()))
    df = df.groupby(['author_id']).agg(collect_list(struct("max_answer_created", "author_url_token", "sum_count")).alias('struct')) \
    .withColumn('struct', my_udf('struct'))
    df = df.cache()  # 多用cache以减少重复计算
    df = df.withColumn('url_token', col('struct').getItem(0)).withColumn('old_url_token', col('struct').getItem(1)).withColumn('sum_count', col('struct').getItem(2)).withColumn('max_answer_created', col('struct').getItem(3))

    df_user = df.select(['author_id', 'url_token', 'old_url_token', 'sum_count', 'max_answer_created'])
    """

    print(df.show())
    print(df.count())


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

    main()
