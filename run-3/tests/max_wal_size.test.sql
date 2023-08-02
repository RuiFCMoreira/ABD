-- *
alter system set max_wal_size = "2GB";
select pg_reload_conf();