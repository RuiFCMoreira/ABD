drop index title_id_hash_idx;
drop index title_type_title_idx;
drop index start_year_title_idx;
drop index last_seen_uh;
drop index search_title;
drop index tid_region_titleAkas;

alter system set work_mem = '4MB';
alter system set shared_buffers = '128MB';

alter system set max_wal_size = "1GB";

select pg_reload_conf();