#!/usr/bin/python3
from operator import itemgetter
import sys

current_type = None
current_quantity = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    trade_type, quantity = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        quantity = int(quantity)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_type == trade_type:
        current_quantity += quantity
    else:
        if current_type:
            # write result to STDOUT
            print ('%s\t%s' % (current_type, current_quantity))
        current_type = trade_type
        current_quantity = quantity

# do not forget to output the last word if needed!
if current_type == trade_type:
    print ('%s\t%s' % (current_type, current_quantity))
