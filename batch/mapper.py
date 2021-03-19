#!/usr/bin/python3
import sys
import json

# input comes from STDIN (standard input)
for order in sys.stdin:
    # remove leading and trailing whitespace
    order = json.loads(order.strip())
    # split the line into words
    #words = line.split()
    trade_type = order["message"]["trade_type"]
    order_quantity = order["message"]["order_quantity"]
    #print (trade_type)
    # increase counters
    #for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
    print ("%s\t%s"%(trade_type, order_quantity))

