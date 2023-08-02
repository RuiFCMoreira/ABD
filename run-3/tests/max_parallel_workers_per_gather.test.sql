-- *
alter system set max_parallel_workers_per_gather = 4;
select pg_reload_conf();