from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from pprint import pprint

class MySubscribeCallback(SubscribeCallback):
    def status(self, pubnub, status):
        pass

    def presence(self, pubnub, presence):
        pprint(presence.__dict__)

    def message(self, pubnub, event):
        pprint(event.__dict__)
        print(event)
        print("{} {} ".format(event.message["bid_price"], event.message["symbol"]))

def my_publish_callback(envelope, status):
    print(envelope, status)

pnconfig = PNConfiguration()
pnconfig.subscribe_key = "sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe" 
#pnconfig.publish_key = "pub-c-55ccbbde-1227-43f3-ab85-e83a353b3ede"

pubnub = PubNub(pnconfig)

pubnub.add_listener(MySubscribeCallback())

pubnub.subscribe()\
    .channels("pubnub-market-orders")\
    .with_presence()\
    .execute()\

