from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from pprint import pprint
from kafka import KafkaProducer
import json
from kafka.admin import KafkaAdminClient, NewTopic

producer = KafkaProducer(bootstrap_servers=["pi-node11:9092"])


class MySubscribeCallback(SubscribeCallback):
    def status(self, pubnub, status):
        pass

    def presence(self, pubnub, presence):
        pass

    def message(self, pubnub, event):
        data = event.__dict__
        msg = data["message"]
        get_msg = [','.join(map(str,msg.values()))][0].split("\"")[0]
        print ("------------------------ \n", json.dumps( msg ).encode())
        #producer.send("market", json.dumps(((str(msg.values())))).encode())
        producer.send("market", json.dumps( msg ).encode())

def my_publish_callback(envelope, status):
    pass#print(envelope, status)

def create_topic(topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')
    topic_list = []
    topic_list.append(NewTopic(name="example_topic", num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)


pnconfig = PNConfiguration()
pnconfig.subscribe_key = "sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe" 
#pnconfig.publish_key = "pub-c-55ccbbde-1227-43f3-ab85-e83a353b3ede"

pubnub = PubNub(pnconfig)

pubnub.add_listener(MySubscribeCallback())

pubnub.subscribe()\
    .channels("pubnub-market-orders")\
    .with_presence()\
    .execute()\

