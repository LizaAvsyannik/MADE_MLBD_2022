#!/usr/bin/python3

import sys

sum_sq = 0
sum = 0
count = 0

# using formula formula for variance = sum(X^2) / N - mean^2

for line in sys.stdin:
    var_chunck, mean_chunck, count_chunck \
        = list(map(float, line.strip().split('\t')))

    sum += mean_chunck * count_chunck
    sum_sq += (var_chunck + mean_chunck ** 2) * count_chunck
    count += count_chunck

mean = sum / count
var = sum_sq / count - mean ** 2
print(var)
