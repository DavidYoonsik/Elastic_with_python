from elasticsearch import Elasticsearch
import json


es = Elasticsearch(hosts=[{'host': '192.168.16.244', 'port': 9200}])

if __name__ == "__main__":

    page = es.search(index='seoul-metro-passenger-2*', scroll='10m', size=10000, body='{"query": {"match_all": {}}}')

    query_request = []

    sid = page['_scroll_id']

    while(int(page['hits']['total']) > 0):
        print("Scrolling...", str(int(page['hits']['total'])))
        query_request.append(page['hits']['hits'])
        page = es.scroll(scroll_id=sid, scroll='10m')
        page['hits']['total'] = len(page['hits']['hits'])


    print(len(query_request))

    result = []

    for i in query_request:
        for j in i:
            result.append(j['_source'])

    print(len(result))

    with open("/home/data/tube_time.json", "w") as fw:
        fw.write(json.dumps(result))


'''
    스파크 사용하는 방법은 여기 명령어를 따라서 합니다.
    Test : 192.168.2.12 에서 진행.
    
    "Spark_Sql Join with pyspark"
    
    01. pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1
    
    02. from pyspark.sql import SparkSession
    
    03. spark = SparkSession.builder.getOrCreate()
    
    04. df = spark.read.json("/home/data/tube_time.json")
    
    05. df.show()
    
    06. df2 = spark.read.json("/home/data/tube_code.json")
    
    07. df.createGlobalTempView("t")

    08. df2 = df2.filter(df2.station_cd.isNotNull())
    
    09. df2.createGlobalTempView("c")
    
    10. result = spark.sql(" \
            select t.ALIGHT_PASGR_NUM, t.LINE_NUM, t.RIDE_PASGR_NUM, t.SUB_STA_NM, t.USE_DT, t.WORK_DT, c.xpoint_wgs, c.ypoint_wgs \
            from global_temp.t as t, global_temp.c as c \
            where t.SUB_STA_NM=c.station_nm \
        ")
    
    11. result.write.json("/home/data/result.json")
    
'''