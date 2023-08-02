alter system set work_mem = '10MB';
alter system set shared_buffers = '3.2GB';

alter system set max_wal_size = "4GB";

select pg_reload_conf();