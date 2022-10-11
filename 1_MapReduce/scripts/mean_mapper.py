#!/usr/bin/python3

import sys

sum = 0
count = 0

for line in sys.stdin:
    sum += int(line)
    count += 1

print('%s\t%s' % (sum / count, count))
