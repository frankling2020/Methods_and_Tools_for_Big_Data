#!/bin/bash

# set -x

for num in 10 20 50 100 200 500 1000 2000 5000
do
	echo $num >> record.txt
	python3 generator.py $num
	echo `wc -l scores.csv` >> record.txt
	bin/hdfs dfs -rm input/scores.csv
	bin/hdfs dfs -put scores.csv input
	{ time bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-3.3.2.jar -files mapper.sh,reducer.sh -input input/scores.csv -output /score_$(date +%s) -mapper "mapper.sh" -reducer "reducer.sh" 2>&1; } > 111.txt 2>&1
	if [[ $? -ne 0 ]]; then
		echo "fail!" >> record.txt
	fi
	grep -e "real" -e "Submitted application" 111.txt >> record.txt
	echo "=====================================================" >> record.txt
done

