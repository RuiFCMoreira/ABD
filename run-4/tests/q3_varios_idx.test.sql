-- 3
create index tpc_title_name on titlePrincipalsCharacters (title_id, name_id);
create index tp_categoty_id on titlegenre using hash (category_id);

alter system set work_mem = '20MB';
alter system set max_parallel_workers_per_gather = 4;
alter system set shared_buffers = '2GB';

select pg_reload_conf();