#!/bin/bash

CLIENTS=(1 4 8 16 24 32)
# CLIENTS=(16 24 32)
BENCHMARK_FOLDER=/home/imdbBench/transactional/
JAR_FOLDER=$BENCHMARK_FOLDER/target
TESTS_FOLDER=transactional-tests
RESULTS_FOLDER=transactional-results
DATABASE_NAME=dbdump
DB_DUMP_FOLDER=/home

mkdir $RESULTS_FOLDER

echo "Starting at $(date +%T)"

mvn -f $BENCHMARK_FOLDER/pom.xml clean package #build benchmark jar

echo "Running preparation script" && sudo sh -c "sudo -u postgres psql -U postgres -d $DATABASE_NAME -a -c \"ALTER USER postgres WITH PASSWORD 'postgres'\";"

for filename in $TESTS_FOLDER/*.test.sql; do
    echo "Running $filename"

    IFS=" " read -a queries <<< $(head -n 1 $filename) # load queries from first line of test file
    # echo "Running queries $queries"

    # echo "Preparation with $filename"
    echo "Running preparation script" && sudo sh -c "sudo -u postgres psql -U postgres -d $DATABASE_NAME -a -f $filename"

    for c in "${CLIENTS[@]}"; do
        /bin/bash reload.sh $DATABASE_NAME $DB_DUMP_FOLDER
        /bin/bash clear_cache.sh $DATABASE_NAME
        echo "$(date +%T) - $(basename "$filename" .test.sql) - $c Clients "
        RESULTS_FILE_NAME="$RESULTS_FOLDER/$(basename "$filename" .test.sql)-$c.txt"
        
        # run benchmark
        java -jar $JAR_FOLDER/transactional-1.0-SNAPSHOT.jar -d jdbc:postgresql://localhost:5432/$DATABASE_NAME -U postgres -P postgres -W 30 -R 240 -c $c > $RESULTS_FILE_NAME

        # results
        echo "Took $(cat $RESULTS_FILE_NAME | awk -v ORS=";" '/(throughput)|(response)/  {print $NF}')"
    done

    # echo "tests/$(basename "$filename" .test.sql).cleanup.sql"
    sudo echo "Running cleanup script" && sh -c "sudo -u postgres psql -U postgres -d $DATABASE_NAME -a -f $TESTS_FOLDER/$(basename "$filename" .test.sql).cleanup.sql"
done

/bin/bash reload.sh $DATABASE_NAME $DB_DUMP_FOLDER # reload database

echo "Finished at $(date +%T)"