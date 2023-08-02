drop index akas_id_region_idx;
drop index userHistory_title_id;

alter system set max_parallel_workers_per_gather = 2;
alter system set work_mem = '4MB';

select pg_reload_conf();