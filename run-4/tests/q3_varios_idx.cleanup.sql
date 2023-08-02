drop index tpc_title_name;
drop index tp_categoty_id;

alter system set work_mem = '4MB';
alter system set max_parallel_workers_per_gather = 2;
alter system set shared_buffers = '128MB';

select pg_reload_conf();