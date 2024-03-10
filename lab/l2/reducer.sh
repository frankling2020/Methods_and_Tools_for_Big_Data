#!/bin/bash
sort -r -t $'\t' -k 1 -k 2 | awk -F $'\t' '!a[$1]++'
exit 0

