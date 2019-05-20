from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import time, datetime
import os



# es = Elasticsearch(['192.168.16.246:9200'], timeout=60, retry_on_timeout=True)
es = Elasticsearch(['192.168.2.12:9200'], timeout=60, retry_on_timeout=True)

request_body = {
            "mappings": {
                "_doc": {
                    "properties": {
                        "USE_DT": {
                            "type": "date"
                        },
                        "location": {
                            "type": "geo_point"
                        }
                        # "dest_port": {
                        #     "type": "text"
                        # }
                    }

                }
            }
        }

if __name__ == "__main__":
    # res = es.indices.delete(index='seoul-metro-passenger-geo2')
    es.indices.create(index='seoul-metro-passenger-geo', body=request_body)

    es_data = []

    if es is not None:
        path_to_json = '/home/data/ressssssssssssssssss'
        json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

        for file in json_files:

            es_data = []

            with open(os.path.join(path_to_json, file), "r") as fr:
                a = fr.readlines()

                for i in a:
                    data = json.loads(i)
                    if 'xpoint_wgs' not in data or 'ypoint_wgs' not in data:
                        continue
                    data['location'] = {
                        'lat': float(data['xpoint_wgs']),
                        'lon': float(data['ypoint_wgs'])
                    }
                    action = {"_index": "seoul-metro-passenger-geo", "_type": "_doc", '_source': data}
                    es_data.append(action)

                    if len(es_data) > 500:
                        helpers.bulk(es, es_data, stats_only=False)
                        es_data = []

                if len(es_data) > 0:
                    helpers.bulk(es, es_data, stats_only=False)
