-- 1
create index akas_id_region_idx on titleAkas (title_id, region);
create index userHistory_title_id on userHistory (title_id);

alter system set max_parallel_workers_per_gather = 4;
alter system set work_mem = '20MB';

select pg_reload_conf();