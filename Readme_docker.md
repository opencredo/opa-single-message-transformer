# Steps to Run 
This is a sample setup to run kafka in docker. There is a volume mapping on data folder where the sink and source configs are stored, as well as the data file to be read by the source connector. 

Note: Sink part is still in progress. Eventually, we want to download the transformer jar into plugins folder and run this repo E2E.  

### Step 0: Run docker-compose

We will start with running docker-compose :
```
$ docker-compose up -d 
```

### Step 1: Check Connectors and Plugings
We will check the connectors and plugins. After waiting a minute or so, the connectors list should result an empty array, and the plugins query should show the kafka connectors along with 2 connectors defined in the docker-compose file, the file source connector and the ES sink connector. 
```
$ docker exec -it connect bash
[appuser@connect ~]$ curl -s 127.0.0.1:8083/connectors
[appuser@connect ~]$ curl -s 127.0.0.1:8083/connector-plugins |grep -Po '"class":.*?[^\\]",'
```

### Step 2: Create Filestream Source Connector  
Now, create the filestream connector, we used mmolimar plugin, for no specific reason. After a minute or so, the newly registed File Source Connector should be displayed.
```
[appuser@connect ~] cd /tmp/data &&
curl -s -X POST -H 'Content-Type: application/json' --data @file-stream-mmolimar.json http://localhost:8083/connectors
[appuser@connect data] curl -s 127.0.0.1:8083/connectors
```
### Step 3: Check the Consumer Messages - WIP
On another terminal window, we will connect to the broker, and verify the consumer receives the messages from the opa-transform topic
```
$ docker exec -it broker bash
[appuser@broker ~] kafka-topics --bootstrap-server broker:9092 --list
[appuser@broker ~] kafka-console-consumer --bootstrap-server broker:9092 --topic opa-transform --from-beginning --max-messages 100 --timeout-ms 20000

Warning : The messages are not being displayed. 
```
### Step 4:  Create the ElasticSearch Sink - WIP 
Create to sink for elastic search 
```
$ docker exec -it connect bash
[appuser@connect ~] cd /tmp/data
[appuser@connect data] curl -s -X POST -H 'Content-Type: application/json' --data @sink-elastic-file-standalone.json http://localhost:8083/connectors
[appuser@connect data] curl -s 127.0.0.1:8083/connectors/sink-elastic-file-standalone/tasks
[appuser@connect data] curl elasticsearch:9200/sink-elastic-file-standalone/_search 
[appuser@connect data] curl elasticsearch:9200/opa-transform/_search 