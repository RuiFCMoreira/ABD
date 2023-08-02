-- *
alter system set shared_buffers = '3.2GB';
select pg_reload_conf();