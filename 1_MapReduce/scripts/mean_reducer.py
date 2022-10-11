#!/usr/bin/python3

import sys

sum = 0
count = 0

for line in sys.stdin:
    mean_chunck, count_chunck = list(map(float, line.strip().split('\t')))
    sum += mean_chunck * count_chunck
    count += count_chunck

print(sum / count)
