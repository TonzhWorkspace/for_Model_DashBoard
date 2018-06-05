# -*- coding: utf-8 -*-
# __author__ = 'Gz'
import json
import geoip2.database
import Geohash.geohash as geohash
from kafka import KafkaConsumer
from influxdb import InfluxDBClient
import os

PATH = os.path.dirname(os.path.abspath(__file__))
consumer = KafkaConsumer('emoji_appstore', bootstrap_servers='kika-data-gimbal0.intranet.com:9092',
                         group_id='Model_DashBoard')
client = InfluxDBClient(host='0.0.0.0', port=8086, username='root', password='root', database='popup_geohash')


def ip_to_genhash(ip):
    # with geoip2.database.Reader(PATH + '/GeoIP2-City.mmdb') as reader:
    with geoip2.database.Reader(
            '/home/guanzhao/for_Model_DashBoard/for_Model_DashBoard/GeoLite2-City_20180501/GeoLite2-City.mmdb') as reader:
        print(ip)
        response = reader.city(ip)
        latitude = response.location.latitude
        longitude = response.location.longitude
        result = geohash.encode(latitude, longitude)
        print(result)
    return result


def one_consumer():
    for msg in consumer:
        msg = msg.value
        if ('"iid":"send"' in msg.decode('utf8')) and ('"l":"keyboard_sticker2_suggestion_pop"' in msg.decode('utf8')):
            msg = msg.decode('utf8').split(',,')
            ip = msg[0].split(',')[-1]
            # log = '{' + str(msg[1].split(',{')[1].split('},')[0]) + '}' + '}'
            # log = log.replace('\\', '')
            # log = log.replace('"{', '{').replace('}"', '}')
            # print(log)
            log = '{' + msg[1].split(',{')[1]
            print(msg)
            print(log)
            log_json = json.loads(log)
            iid = log_json['iid']
            exitra = log_json['extra']
            kb_lang = exitra['kb_lang']
            lang = exitra['lang']
            try:
                sticker_id = exitra['sticker_id']
            except:
                sticker_id = exitra['item_id']
            try:
                tag = exitra['tag']
            except:
                try:
                    tag = exitra['tags']
                except:
                    tag = exitra['key_word']
            genhash_result = ip_to_genhash(ip)
            json_body = [{
                "measurement": "country",
                'tags': {'tag_kb_lang': kb_lang,
                         'tag_lang': lang,
                         'tag_sticker_id': sticker_id,
                         'tag_tag': tag,
                         'geohash': genhash_result},
                "fields": {
                    'tag': tag,
                    'sticker_id': sticker_id,
                    'lang': lang,
                    'kb_lang': kb_lang
                },
            }]
            client.create_database('popup_geohash')
            client.write_points(json_body)


if __name__ == '__main__':
    one_consumer()
