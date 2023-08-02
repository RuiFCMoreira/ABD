-- 2
create index title_hash on title using hash (id);
create index tg_title_id on titlegenre (title_id);

alter system set max_parallel_workers_per_gather = 4;

select pg_reload_conf();