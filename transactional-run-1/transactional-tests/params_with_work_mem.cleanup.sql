alter system set work_mem = '4MB';
alter system set shared_buffers = '128MB';

alter system set max_wal_size = "1GB";

select pg_reload_conf();