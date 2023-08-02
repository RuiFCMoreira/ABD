alter system set max_parallel_workers_per_gather = 2;
select pg_reload_conf();