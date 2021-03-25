import subprocess
import os
import time

# get the data from kafka topic
print("-------------------------subscribe kafka topic and save stream data in JSON------------------")
proc = subprocess.Popen(["kafka-console-consumer.sh", "--bootstrap-server", "pi-node11:9092", "--topic", "market"], stdout=open("data.json",'w+'))
print("waiting for 60 seconds...")
time.sleep(60)
print("killed %d"%(proc.pid))
proc.kill()
os.system("kafka-console-consumer.sh --bootstrap-server pi-node11:9092 --topic market > data.json")
# copy the data into hdfs
print("----------------------------------copy the stream data into hdfs-----------------------------")
os.system("hdfs dfs -rm -r /user/pi/result")
os.system("hdfs dfs -mkdir /user/pi/result")
os.system("hdfs dfs -copyFromLocal ~/projet/BigData/batch/data.json /user/pi/result")
# clean the output hdfs repository
print("----------------------------clear the output directory before the MR job----------------------")
os.system("hdfs dfs -rm -r /user/pi/result_data")
# start the map-reduce job
print("-----------------------------------------submit the MR job------------------------------------")
os.system("hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar -file ~/projet/BigData/batch/mapper.py -mapper ~/projet/BigData/batch/mapper.py -file ~/projet/BigData/batch/reducer.py -reducer ~/projet/BigData/batch/reducer.py -input /user/pi/result/data.json -output /user/pi/result_data")
print("-----------------------------------------result of MR job------------------------------------")
os.system("hdfs dfs -cat /user/pi/result_data/part-00000")
