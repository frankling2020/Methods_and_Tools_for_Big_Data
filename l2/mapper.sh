#!/bin/bash
awk -F ',' '{printf "%s\t%s\n", $2, $3}'

