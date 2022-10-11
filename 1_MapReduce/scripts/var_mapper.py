#!/usr/bin/python3

import sys

sum = 0
sum_sq = 0
count = 0

# using formula formula for variance = sum(X^2) / N - mean^2

for line in sys.stdin:
    n = int(line)
    sum += n
    sum_sq += n ** 2
    count += 1

mean = sum / count
var = sum_sq / count - mean ** 2
print('%s\t%s\t%s' % (var, mean, count))
