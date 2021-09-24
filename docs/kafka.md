
## Kafka shell 命令

```shell
./kafka-topics.sh --zookeeper 10.83.192.6:2181 --list // 查看topic清单
./kafka-console-consumer.sh --bootstrap-server 10.83.192.10:9092,10.83.192.11:9092,10.83.192.12:9092 --topic flink-test --from-beginning  //终端消费
./kafka-console-consumer.sh --zookeeper 10.83.192.6:2181,10.83.192.7:2181,10.83.192.8:2181  --topic flink-topic --from-beginning  //终端消费
./kafka-console-producer.sh --broker-list 10.83.192.10:9092,10.83.192.11:9092,10.83.192.12:9092 --topic flink-test
```


```json
{"name":"swt","age":19,"sex":"F","job":{"job":"boss", "offer":"500"}}
{"name":"swt","age":18,"sex":"F","job":{"job":"dagongzai", "offer":"5"}}
{"name":"swt","age":20,"sex":"F","job":{"job":"dagongzai", "offer":"5"}}
{"name":"swt","age":22,"sex":"M","job":{"job":"boss", "offer":"500"}}
{"name":"swt","age":32,"sex":"M","job":{"job":"boss", "offer":600}}

{"name":"swt","age":19,"sex":"F","ext":"ext"}

```

```json
{
    "id":"250250250",
    "name":"Pelosy",
    "timestamp":"1607333319316",
    "type":"type_1",
    "body":{
        "appVersion":"1.11.1",
        "behavior":"nigger",
        "deviceBrand":"fool",
        "deviceId":"250222",
        "deviceModel":"Block",
        "duration":"222",
        "enabledFlag":"1",
        "location":"Mars",
        "loginPlatform":"mobile",
        "networkType":"lte",
        "osType":"Android",
        "osVersion":"11",
        "pageId":"2",
        "phone":"15068902351",
        "region":"51",
        "remark":"Y",
        "rowAuthCustom":"N",
        "userId":"222002",
        "userName":"ErBi"
    }
}

{"id":"250250555","name":"Silva","timestamp_str":"1607333319316","type":"type_1","body":{"appVersion":"1.11.1","behavior":"Micdonload","deviceBrand":"boss","deviceId":"250Hilory222","deviceModel":"Block","duration":"222","enabledFlag":"1","location":"Mars","loginPlatform":"mobile","networkType":"lte","osType":"Android","osVersion":"11","pageId":"2","phone":"15068902351","region":"51","remark":"Y","rowAuthCustom":"N","userId":"222002","userName":"ErBi"}}
```





