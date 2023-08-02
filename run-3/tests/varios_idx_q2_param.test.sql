-- 2
create index title_type_idx on title (title_type);
create index title_hash on title using hash (id);
create index userHistory_title_id on userHistory (title_id);
create index title_id_primary_title on title (id,primary_title);

alter system set work_mem = '20MB';

alter system set shared_buffers = '2GB';

alter system set random_page_cost = "1.2" ;
select pg_reload_conf();