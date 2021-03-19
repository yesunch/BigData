# Instructions to start a MapReduce Job

## Get the data from API

 `python3 ../get_data.py`

## Save data into a local file

`kafka-console-consumer.sh --bootstrap-server pi-node11:9092 --topic market > data.json`

## Upload the data file into hdfs

`hdfs dfs -copyFromLocal ~/projet/BigData/batch/data.json /user/pi/result`

## Run the MapReduce job

`hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar -file ~/projet/BigData/batch/mapper.py -mapper ~/projet/BigData/batch/mapper.py -file ~/projet/BigData/batch/reducer.py -reducer ~/projet/BigData/batch/reducer.py -input /user/pi/batch/data.json -output /user/pi/result_data2`
