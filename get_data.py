from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from pprint import pprint
#from kafka import KafKaProducer
import json

#producer = KafkaProducer(bootstrap_servers=["localhost:9092", "localhost:9093"])


class MySubscribeCallback(SubscribeCallback):
    def status(self, pubnub, status):
        pass

    def presence(self, pubnub, presence):
        pass

    def message(self, pubnub, event):
        #pprint(event.__dict__)
        data = event.__dict__
        msg = data["message"]


def my_publish_callback(envelope, status):
    pass#print(envelope, status)

pnconfig = PNConfiguration()
pnconfig.subscribe_key = "sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe" 
#pnconfig.publish_key = "pub-c-55ccbbde-1227-43f3-ab85-e83a353b3ede"

pubnub = PubNub(pnconfig)

pubnub.add_listener(MySubscribeCallback())

pubnub.subscribe()\
    .channels("pubnub-market-orders")\
    .with_presence()\
    .execute()\

