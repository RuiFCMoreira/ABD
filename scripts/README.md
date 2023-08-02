# Analytics testing script guide

Use folder `queries` to store all variations of queries (with and without any optimizations)

Use folder `tests` to store the setup for each test ending in .test.sql e.g. creating an index under tests/title_decade_idx.test.sql, as well as the cleanup for each test ending in .cleanup.sql e.g. dropping and index under tests/title_decade_idx.cleanup.sql. The first line of every .test.sql file should have a SQL comment (`--`) followed by a space separated list of matches for the queries. For instance:

`-- 1` will run all queries that start with 1

`-- 1.sql 2.sql` will run queries 1 and 2

`-- *` will run all queries

Cache is automatically cleared before each test.

Use the first argument to specify how many runs of each test should be done.

Execution of the script should be:

`sudo ./analytics_test_files.sh 3 > test_files_output.txt 2>&1`