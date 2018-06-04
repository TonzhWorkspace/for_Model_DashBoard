# -*- coding: utf-8 -*-
# __author__ = 'Gz'
import json
import geoip2.database
import Geohash.geohash as geohash
from kafka import KafkaConsumer
from influxdb import InfluxDBClient

consumer = KafkaConsumer('emoji_appstore', bootstrap_servers='kika-data-gimbal0.intranet.com:9092',
                         group_id='Model_DashBoard')
client = InfluxDBClient(host='0.0.0.0', port=8086, username='root', password='root', database='popup_geohash')


def ip_to_genhash(ip):
    with geoip2.database.Reader('./GeoIP2-City.mmdb') as reader:
        response = reader.city(ip)
        country = response.country.iso_code
        specific = response.subdivisions.most_specific.name
        latitude = response.location.latitude
        longitude = response.location.longitude
        city_name = response.city.name
        # print(country)
        # print(specific)
        # print(city_name)
        result = geohash.encode(latitude, longitude)
        # print(result)
    return result


def one_consumer():
    for msg in consumer:
        if '"iid":"send"' in str(msg):
            msg = str(msg).split(',,')
            ip = msg[0].split(',')[-1]
            log = '{' + str(msg[1].split(',{')[1].split('},')[0]) + '}' + '}'
            log = log.replace('\\', '')
            log = log.replace('"{', '{').replace('}"', '}')
            print(log)
            log_json = json.loads(log)
            iid = log_json['iid']
            kb_lang = log_json['extra']['kb_lang']
            lang = log_json['extra']['lang']
            try:
                sticker_id = log_json['extra']['sticker_id']
            except:
                sticker_id = log_json['extra']['item_id']
            try:
                tag = log_json['extra']['tag']
            except:
                tag = log_json['extra']['tags']
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
