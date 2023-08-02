alter system set max_wal_size = "1GB";
select pg_reload_conf();