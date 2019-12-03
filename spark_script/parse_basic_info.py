# coding=u8
import json
import os
import pyspark
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
import datetime
import sys

try:
    reload(sys)
    sys.setdefaultencoding('u8')
except:
    pass


def map_line(line):
    json_text = json.loads(line)


    # 从20180123 开始的数据用elasticsearch 导出
    if '$date' not in json_text['answer_created']:
        answer_created = json_text['answer_created']
        answer_updated = json_text['answer_updated']
        insert_time = json_text['insert_time']
        #last_update_time = ''
        question_created = json_text['question_created']
    else:
        answer_created = json_text['answer_created']['$date'][:19]
        answer_updated = json_text['answer_updated']['$date'][:19]
        insert_time = json_text['insert_time']['$date'][:19]
        #last_update_time = json_text['last_update_time']['$date']
        question_created = json_text['question_created']['$date'][:19]

    answer_id = json_text['answer_id']
    answer_content = json_text['answer_content']
    author_id = json_text['author_id']
    author_name = json_text['author_name']
    author_url_token = json_text['author_url_token']
    badge_num = json_text['badge_num']
    can_comment = json_text['can_comment']
    comment_count = json_text['comment_count']
    gender = json_text['gender']
    question_id = json_text['question_id']
    question_title = json_text['question_title']
    reward_member_count = json_text['reward_member_count']
    reward_total_money = json_text['reward_total_money']
    voteup_count = json_text['voteup_count']

    ret_tuple = answer_id, answer_content, answer_created, answer_updated, author_id, author_name, author_url_token, badge_num, can_comment,\
            comment_count, gender, insert_time, question_created, question_id,\
            question_title, reward_member_count, reward_total_money, voteup_count

    return ret_tuple

def main(file_name):
    #print(spark.sparkContext.getConf().getAll())

    #file_name = '20180109_20180112'
    rdd = sc.textFile("file:////data/zhihu/zhihu2/%s.json"%file_name)
    rdd = rdd.map(map_line)
    column_list = ['answer_id', 'answer_content', 'answer_created', 'answer_updated', 'author_id', 'author_name', 'author_url_token', 'badge_num',\
            'can_comment', 'comment_count', 'gender', 'insert_time', 'question_created', 'question_id',\
            'question_title', 'reward_member_count', 'reward_total_money', 'voteup_count']

    schema = StructType([StructField('answer_id', LongType(), False),
        StructField('answer_content', StringType(), True),
        StructField('answer_created', StringType(), False),
        StructField('answer_updated', StringType(), False),
        StructField('author_id', StringType(), False),
        StructField('author_name', StringType(), False),
        StructField('author_url_token', StringType(), False),
        StructField('badge_num', IntegerType(), True),
        StructField('can_comment', BooleanType(), True),
        StructField('comment_count', IntegerType(), True),
        StructField('gender', ByteType(), True),
        StructField('insert_time', StringType(), False),
        StructField('question_created', StringType(), False),
        StructField('question_id', LongType(), False),
        StructField('question_title', StringType(), True),
        StructField('reward_member_count', IntegerType(), True),
        StructField('reward_total_money', IntegerType(), True),
        StructField('voteup_count', IntegerType(), True)])

    #df = rdd.toDF(column_list)
    df = spark.createDataFrame(rdd, schema)

    print(df.printSchema())
    #print(df.show(30, False))
    #df.write.mode('overwrite').json('hdfs://device1/zhihu/basic_info2/%s/'%file_name)

    df.write.mode('overwrite').parquet('hdfs://device1/zhihu/basic_info/%s/'%file_name)

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
    spark.sparkContext.setLogLevel('WARN')



    if len(sys.argv) < 2 :
        for file_ in os.listdir("/data/zhihu/zhihu2/"):
            if file_.endswith(".json"):
                file_name = file_.split('.')[0]
                #path = os.path.join("/data/zhihu/zhihu2/", file)
                #if '2018110' in file_name:
                main(file_name)

        sys.exit()
    else:
        file_name = sys.argv[1]
        main(file_name)


