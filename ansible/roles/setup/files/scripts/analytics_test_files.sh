#!/bin/bash

DATABASE_NAME=dbdump
RESULTS_FOLDER=results
NUMBER_OF_RUNS=${1:-3}
mkdir $RESULTS_FOLDER

echo "Starting at $(date +%T)"

for filename in tests/*.test.sql; do
    echo "Running $filename"

    IFS=" " read -a queries <<< $(head -n 1 $filename)
    # echo "Running queries $queries"

    # echo "Preparation with $filename"
    echo "Running preparation script" && sudo sh -c "sudo -u postgres psql -U postgres -d $DATABASE_NAME -a -f $filename"


    for query in "${queries[@]:1}"; do # skip comment
        for queryfile in queries/$query*; do # for all queries that match
            for ((i=1; i<=$NUMBER_OF_RUNS; i++)); do
                /bin/bash clear_cache.sh $DATABASE_NAME
                echo "$(date +%T) - $(basename "$filename" .test.sql) - query $(basename "$queryfile" .sql) - Run $i "
                RESULTS_FILE_NAME="$RESULTS_FOLDER/$(basename "$filename" .test.sql)-$(basename "$queryfile" .sql)-$i.txt"
                # echo "$RESULTS_FILE_NAME"
                echo "Running query" && sudo sh -c "sudo -u postgres psql -U postgres -d $DATABASE_NAME -a -f $queryfile > $RESULTS_FILE_NAME"
                echo "Took $(cat $RESULTS_FILE_NAME | awk '/Execution Time:/ {print $3 $4}')"
            done
        done
    done

    # echo "tests/$(basename "$filename" .test.sql).cleanup.sql"
    sudo echo "Running cleanup script" && sh -c "sudo -u postgres psql -U postgres -d $DATABASE_NAME -a -f tests/$(basename "$filename" .test.sql).cleanup.sql"
done

echo "Finished at $(date +%T)"