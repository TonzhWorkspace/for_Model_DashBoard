# -*- coding: utf-8 -*-
# __author__ = 'Gz'
from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, create_map
from pyspark.sql.types import StringType, MapType, IntegerType, StructType, ArrayType
import os
import datetime
import json
import requests

# os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
# createTime = '2018-05-31'
createTime = str(datetime.date.today() - datetime.timedelta(days=1))
today = createTime.replace('-', '')


# spark部分
def get_scenario(data):
    if data.scenario != None:
        return data.scenario
    else:
        try:
            return json.loads(data.extra)['scenario']
        except:
            return None


def get_sticker(data):
    return data.itemId


def get_ADPClassName(data):
    return data.ADPClassName


def get_item(data):
    return data.itemId


def get_click(data):
    return data.y


def get_bucket(data):
    try:
        return json.loads(data.extra.replace("\\", ''))['bucket'].split(':')[1]
    except:
        return data.bucketName


def get_spark_data():
    # spark = SparkSession.builder.master('local').appName('gztest').getOrCreate()
    spark = SparkSession.builder.appName('for_DashBoard').getOrCreate()
    path = 'hdfs://kika-data-hadoop00.intranet.com:9000/daiqiang/gbdt_rmd-plt-monitor/' + today + '*/*'
    table = spark.read.format('json').load(path=path)
    t_p_data = table.toDF('category', 'day', 'duid', 'extra', 'flowExtra',
                          'operation', 'paramExtra', 'sessionId', 'timestamp').sort('timestamp',
                                                                                    ascending=False).persist()
    # 清理没有sessionId的和非train和predicet的数据
    t_p_data = t_p_data.filter(t_p_data.sessionId != '').persist()
    # 添加item
    f_get_t_item = udf(get_item, returnType=StringType())
    t_p_data = t_p_data.withColumn('item', f_get_t_item(t_p_data.paramExtra)).persist()
    # 添加scenario
    f_scenario = udf(get_scenario, returnType=StringType())
    t_p_data = t_p_data.withColumn('scenario', f_scenario(t_p_data.extra)).persist()
    t_p_data = t_p_data.filter(t_p_data.scenario != '').persist()
    # 添加sticker
    f_sticker = udf(get_sticker, StringType())
    t_p_data = t_p_data.withColumn('sticker', f_sticker(t_p_data.paramExtra)).persist()
    # 添加click
    f_click = udf(get_click, StringType())
    t_p_data = t_p_data.withColumn('click', f_click(t_p_data.extra)).persist()
    # 添加bucket
    f_scenario = udf(get_bucket, returnType=StringType())
    t_p_data = t_p_data.withColumn('bucket', f_scenario(t_p_data.extra)).persist()
    # 训练数据
    t_data = t_p_data.filter(t_p_data.operation == 'train').persist()
    # 预测数据
    p_data = t_p_data.filter(t_p_data.operation == 'predict').persist()
    # miss
    p_miss_data = t_p_data.filter(t_p_data.operation == 'predict-miss').persist()
    # 有预测又有训练的sessionId
    t_sessionId = t_data.select('sessionId')
    p_sessionId = p_data.select('sessionId')
    t_p_sessionId_list = [t_p.sessionId for t_p in list(
        t_sessionId.select('sessionId').intersect(p_sessionId.select('sessionId')).distinct().toLocalIterator())]
    # 预测中有训练的数据
    p_t_and_p_data = p_data.filter(p_data.sessionId.isin(t_p_sessionId_list)).persist()
    # bucket
    # bucket show number
    bucket_show_numbr = t_data.select('sessionId', 'bucket').distinct().groupBy('bucket').count().toDF('bucket',
                                                                                                       'n_count')
    bucket_show_numbr = bucket_show_numbr.rdd.map(
        lambda bucket_show_numbr: '{"bucket":"' + str(bucket_show_numbr.bucket) + '","count_n": "' + str(
            bucket_show_numbr.n_count) + '"}').collect()
    # bucket not send number
    bucket_not_send_number = t_data.filter(t_data.click == -1.0).select('sessionId', 'bucket').distinct().groupBy(
        'bucket').count().toDF('bucket', 'n_count')
    bucket_not_send_number = bucket_not_send_number.rdd.map(
        lambda bucket_not_send_number: '{"bucket":"' + str(bucket_not_send_number.bucket) + '","count_n": "' + str(
            bucket_not_send_number.n_count) + '"}').collect()
    # bucket send number
    bucket_send_number = t_data.filter(t_data.click == 1.0).select('sessionId', 'bucket').distinct().groupBy(
        'bucket').count().toDF('bucket', 'n_count')
    bucket_send_number = bucket_send_number.rdd.map(
        lambda bucket_send_number: '{"bucket":"' + str(bucket_send_number.bucket) + '","count_n": "' + str(
            bucket_send_number.n_count) + '"}').collect()
    # bucket duid number
    bucket_duid_numbr = t_data.select('duid', 'bucket').distinct().groupBy('bucket').count().toDF('bucket', 'n_count')
    bucket_duid_numbr = bucket_duid_numbr.rdd.map(
        lambda bucket_duid_numbr: '{"bucket":"' + str(bucket_duid_numbr.bucket) + '","count_n": "' + str(
            bucket_duid_numbr.n_count) + '"}').collect()
    bucket_result = {'bucket_show_numbr': bucket_show_numbr, 'bucket_send_number': bucket_send_number,
                     'bucket_not_send_number': bucket_not_send_number, 'bucket_duid_number': bucket_duid_numbr}
    # scenario
    # scenario sessionId number
    scenario_sessionId_number = t_p_data.select('sessionId', 'scenario').distinct().distinct().groupBy(
        'scenario').count().toDF(
        'scenario', 'n_count')
    scenario_sessionId_number = scenario_sessionId_number.rdd.map(
        lambda scenario_sessionId_number: '{"scenario":"' + str(
            scenario_sessionId_number.scenario) + '","count_n": "' + str(
            scenario_sessionId_number.n_count) + '"}').collect()
    # scenario p sessionId number
    scenario_p_sessionId_number = p_data.select('sessionId', 'scenario').distinct().groupBy('scenario').count().toDF(
        'scenario', 'n_count')
    scenario_p_sessionId_number = scenario_p_sessionId_number.rdd.map(
        lambda scenario_p_sessionId_number: '{"scenario":"' + str(
            scenario_p_sessionId_number.scenario) + '","count_n": "' + str(
            scenario_p_sessionId_number.n_count) + '"}').collect()
    # scenario t sessionId number
    scenario_t_sessionId_number = t_data.select('sessionId', 'scenario').distinct().groupBy('scenario').count().toDF(
        'scenario', 'n_count')
    scenario_t_sessionId_number = scenario_t_sessionId_number.rdd.map(
        lambda scenario_t_sessionId_number: '{"scenario":"' + str(
            scenario_t_sessionId_number.scenario) + '","count_n": "' + str(
            scenario_t_sessionId_number.n_count) + '"}').collect()
    # scenario p_and t sessionId
    scenario_p_and_t_sessionId_number = p_t_and_p_data.select('sessionId', 'scenario').distinct().groupBy(
        'scenario').count().toDF(
        'scenario', 'n_count')
    scenario_p_and_t_sessionId_number = scenario_p_and_t_sessionId_number.rdd.map(
        lambda scenario_p_and_t_sessionId_number: '{"scenario":"' + str(
            scenario_p_and_t_sessionId_number.scenario) + '","count_n": "' + str(
            scenario_p_and_t_sessionId_number.n_count) + '"}').collect()
    # scenario duid tag
    scenario_duid_tag = p_t_and_p_data.select('scenario', 'duid', 'category').distinct().groupBy(
        'scenario', 'duid', 'category').count().toDF('scenario', 'duid', 'category', 'count_n')
    scenario_duid_tag_all_count = scenario_duid_tag.select('scenario', 'count_n').groupBy('scenario').count().toDF(
        'scenario', 'count_n')
    scenario_duid_tag_all_count = scenario_duid_tag_all_count.rdd.map(
        lambda scenario_duid_tag_all_count: '{"scenario":"' + str(
            scenario_duid_tag_all_count.scenario) + '","count_n": "' + str(
            scenario_duid_tag_all_count.count_n) + '"}').collect()
    # once duid tag
    scenario_duid_tag_once_count = scenario_duid_tag.select('scenario', 'count_n').filter(
        scenario_duid_tag.count_n == 1).groupBy('scenario').count().toDF('scenario', 'count_n')
    scenario_duid_tag_once_count = scenario_duid_tag_once_count.rdd.map(
        lambda scenario_duid_tag_once_count: '{"scenario":"' + str(
            scenario_duid_tag_once_count.scenario) + '","count_n": "' + str(
            scenario_duid_tag_once_count.count_n) + '"}').collect()
    # once duid tag item
    scenario_duid_tag_item = p_t_and_p_data.select('scenario', 'duid', 'category', 'item').distinct() \
        .groupBy('scenario', 'duid', 'category').count().toDF('scenario', 'duid', 'category', 'count_n').select(
        'scenario', 'count_n')
    scenario_duid_tag_item_once_count = scenario_duid_tag_item.filter(scenario_duid_tag_item.count_n == 1).groupBy(
        'scenario').count().toDF('scenario', 'count_n')
    scenario_duid_tag_item_once_count = scenario_duid_tag_item_once_count.rdd.map(
        lambda scenario_duid_tag_item_once_count: '{"scenario":"' + str(
            scenario_duid_tag_item_once_count.scenario) + '","count_n": "' + str(
            scenario_duid_tag_item_once_count.count_n) + '"}').collect()
    # more duid tag tiem
    scenario_duid_tag_item_more_count = scenario_duid_tag_item.filter(scenario_duid_tag_item.count_n > 1).groupBy(
        'scenario').count().toDF('scenario', 'count_n')
    scenario_duid_tag_item_more_count = scenario_duid_tag_item_more_count.rdd.map(
        lambda scenario_duid_tag_item_more_count: '{"scenario":"' + str(
            scenario_duid_tag_item_more_count.scenario) + '","count_n": "' + str(
            scenario_duid_tag_item_more_count.count_n) + '"}').collect()
    scenario_result = {'scenario_sessionId_number': scenario_sessionId_number,
                       'scenario_p_sessionId_number': scenario_p_sessionId_number,
                       'scenario_t_sessionId_number': scenario_t_sessionId_number,
                       'scenario_p_and_t_sessionId_number': scenario_p_and_t_sessionId_number,
                       'scenario_duid_tag_all_count': scenario_duid_tag_all_count,
                       'scenario_duid_tag_once_count': scenario_duid_tag_once_count,
                       'scenario_duid_tag_item_once_count': scenario_duid_tag_item_once_count,
                       'scenario_duid_tag_item_more_count': scenario_duid_tag_item_more_count}
    return {'bucket': bucket_result, 'scenario': scenario_result}


def bucket_data(data):
    result = {}
    bucket_show_numbr = {}
    bucket_send_number = {}
    bucket_duid_numbr = {}
    bucket_not_send_number = {}
    for i in data['bucket_show_numbr']:
        bucket_show_numbr.update(
            {json.loads(i)['bucket']: int(json.loads(i)['count_n'])})
    for i in data['bucket_send_number']:
        bucket_send_number.update(
            {json.loads(i)['bucket']: int(json.loads(i)['count_n'])})
    for i in data['bucket_not_send_number']:
        bucket_not_send_number.update(
            {json.loads(i)['bucket']: int(json.loads(i)['count_n'])})
    for i in data['bucket_duid_number']:
        bucket_duid_numbr.update(
            {json.loads(i)['bucket']: int(json.loads(i)['count_n'])})
    bucket_list = list(bucket_show_numbr.keys())
    for bucket in bucket_list:
        temp = {}
        try:
            temp.update({'bucket_show_numbr': bucket_show_numbr[bucket]})
        except:
            temp.update({'bucket_show_numbr': 0})
        try:
            temp.update({'bucket_send_number': bucket_send_number[bucket]})
        except:
            temp.update({'bucket_send_number': 0})
        try:
            temp.update({'bucket_not_send_number': bucket_not_send_number[bucket]})
        except:
            temp.update({'bucket_not_send_number': 0})
        try:
            temp.update({'bucket_duid_numbr': bucket_duid_numbr[bucket]})
        except:
            temp.update({'bucket_duid_numbr': 0})
        result.update({bucket: temp})
    return result


def scenario_data(data):
    result = {}
    scenario_sessionId_number = {}
    scenario_p_sessionId_number = {}
    scenario_t_sessionId_number = {}
    scenario_p_and_t_sessionId_number = {}
    scenario_duid_tag_all_count = {}
    scenario_duid_tag_once_count = {}
    scenario_duid_tag_item_more_count = {}
    for i in data['scenario_sessionId_number']:
        scenario_sessionId_number.update(
            {json.loads(i)['scenario']: int(json.loads(i)['count_n'])})
    for i in data['scenario_p_sessionId_number']:
        scenario_p_sessionId_number.update(
            {json.loads(i)['scenario']: int(json.loads(i)['count_n'])})
    for i in data['scenario_t_sessionId_number']:
        scenario_t_sessionId_number.update(
            {json.loads(i)['scenario']: int(json.loads(i)['count_n'])})
    for i in data['scenario_p_and_t_sessionId_number']:
        scenario_p_and_t_sessionId_number.update(
            {json.loads(i)['scenario']: int(json.loads(i)['count_n'])})
    for i in data['scenario_duid_tag_all_count']:
        scenario_duid_tag_all_count.update(
            {json.loads(i)['scenario']: int(json.loads(i)['count_n'])})
    for i in data['scenario_duid_tag_once_count']:
        scenario_duid_tag_once_count.update(
            {json.loads(i)['scenario']: int(json.loads(i)['count_n'])})
    for i in data['scenario_duid_tag_item_more_count']:
        scenario_duid_tag_item_more_count.update(
            {json.loads(i)['scenario']: int(json.loads(i)['count_n'])})
    scenario_list = list(scenario_sessionId_number.keys())
    for scenario in scenario_list:
        temp = {}
        try:
            temp.update({'scenario_sessionId_number': scenario_sessionId_number[scenario]})
        except:
            temp.update({'scenario_sessionId_number': 0})
        try:
            temp.update({'scenario_p_sessionId_number': scenario_p_sessionId_number[scenario]})
        except:
            temp.update({'scenario_p_sessionId_number': 0})
        try:
            temp.update({'scenario_t_sessionId_number': scenario_t_sessionId_number[scenario]})
        except:
            temp.update({'scenario_t_sessionId_number': 0})
        try:
            temp.update({'scenario_p_and_t_sessionId_number': scenario_p_and_t_sessionId_number[scenario]})
        except:
            temp.update({'scenario_p_and_t_sessionId_number': 0})
        try:
            temp.update({'scenario_duid_tag_all_count': scenario_duid_tag_all_count[scenario]})
        except:
            temp.update({'scenario_duid_tag_all_count': 0})
        try:
            temp.update({'scenario_duid_tag_once_count': scenario_duid_tag_once_count[scenario]})
        except:
            temp.update({'scenario_duid_tag_once_count': 0})
        try:
            temp.update({'scenario_duid_tag_item_more_count': scenario_duid_tag_item_more_count[scenario]})
        except:
            temp.update({'scenario_duid_tag_item_more_count': 0})
        result.update({scenario: temp})
    return result


def push_bucket_data(bucket_name, show_number, not_send_number, send_number, duid_number):
    url = 'http://34.214.222.244:12000/model-dashboard/monitor/report/create/json'
    # url = 'http://172.31.23.134:12000/model-dashboard/monitor/report/create/json'
    data = {'bucketName': bucket_name, 'algHitPop': show_number, 'algHitSend': send_number,
            'algCtr': int(send_number) / int(show_number), 'algDuid': duid_number,
            'createTime': createTime}
    print(data)
    response = requests.put(url=url, json=data)
    print(response.text)


def run_bucket_push(bucket_data):
    for key, value in bucket_data.items():
        bucket_name = key
        show_number = value['bucket_show_numbr']
        send_number = value['bucket_send_number']
        duid_number = value['bucket_duid_numbr']
        not_send_number = value['bucket_not_send_number']
        print('bucket_name', 'show', 'not_send', 'send', 'duid')
        print(bucket_name, show_number, not_send_number, send_number, duid_number)
        push_bucket_data(bucket_name, show_number, not_send_number, send_number, duid_number)


if __name__ == '__main__':
    get_data = get_spark_data()
    # print(get_data)
    get_bucket_data = bucket_data(get_data['bucket'])
    get_scenario_data = scenario_data(get_data['scenario'])
    # print(get_bucket_data)
    run_bucket_push(get_bucket_data)
    # print(scenario_data(get_data['scenario']))
    # push_bucket_data('EnUsBeforeMod2Bucket', '245372', '242840', '2532', '2895')
