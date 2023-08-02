-- 3_opt
create index n_id_idx on name (id)
alter system set  work_mem = "20MB" ;
alter system set shared_buffers = '3.2GB';
select pg_reload_conf();