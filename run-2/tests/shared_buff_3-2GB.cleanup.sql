alter system set shared_buffers = '128MB';
select pg_reload_conf();