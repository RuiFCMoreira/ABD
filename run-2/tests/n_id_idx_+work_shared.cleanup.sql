drop index n_id_idx ;
alter system set  work_mem = "4MB" ;
alter system set shared_buffers = '128MB';
select pg_reload_conf();