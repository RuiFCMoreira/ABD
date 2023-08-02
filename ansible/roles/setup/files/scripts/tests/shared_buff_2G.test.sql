-- *
alter system set shared_buffers = '2GB';
select pg_reload_conf();