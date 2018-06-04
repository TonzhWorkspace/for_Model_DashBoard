# -*- coding: utf-8 -*-
# __author__ = 'Gz'
from sanic import Sanic
from sanic.response import json as sanic_json
import json
import time
import requests
from beaker.cache import cache_regions, cache_region
from sanic import Blueprint

cache_regions.update({
    'memory': {
        'expire': 600,
        'type': 'memory'
    }
})

bp = Blueprint('DashBoard', url_prefix='/DashBoard')


def time_to_time_stamp(data):
    result = int(str(int(time.mktime(time.strptime(data, '%Y%m%d')))) + '000')
    return result


@cache_region('memory')
def get_target_and_target_data():
    result = {}
    # 外网
    url = 'http://34.214.222.244:12000/model-dashboard/monitor/report/days/14'
    # 内网
    # url = 'http://172.31.23.134:12000/model-dashboard/monitor/report/days/14'
    data = requests.get(url)
    response_data = json.loads(data.text)['data']
    show_target_list = list(response_data.keys())
    target_list = list(response_data[show_target_list[0]][0].keys())
    target_list.remove('id')
    target_list.remove('createTime')
    target_list.remove('bucketName')
    search_target_list = []
    for target in target_list:
        for show_target in show_target_list:
            search_target_list.append(target + '_' + show_target)
    for target in search_target_list:
        temp_show_target = {}
        data_list = response_data[target.split('_')[1]]
        for data in data_list:
            temp_show_target.update({data['createTime']: data[target.split('_')[0]]})
        result.update({target: temp_show_target})
    return {'target_list': search_target_list, 'target_data': result}


@bp.route("/")
async def test(request):
    return sanic_json('ok')


@bp.post("/search")
async def dashboard_search(request):
    print(request.json)
    result = get_target_and_target_data()
    return sanic_json(result['target_list'])


# 展示只支持timeserie
@bp.route("/query", methods=["POST"])
async def dashboard_query(request):
    result = []
    targets = [i['target'] for i in json.loads(request.body.decode())['targets']]
    for target in targets:
        data = get_target_and_target_data()['target_data'][target]
        temp_datapoints = []
        for key, value in sorted(data.items()):
            temp_datapoints.append(
                [float(value), int(str(int(time.mktime(time.strptime(key, '%Y-%m-%d')))) + '000')])
        result.append({"target": target, "datapoints": temp_datapoints})
    return sanic_json(result)


if __name__ == "__main__":
    app = Sanic()
    app.blueprint(bp, url_prefix='/DashBoard')
    app.run(host="0.0.0.0", port=8000)
