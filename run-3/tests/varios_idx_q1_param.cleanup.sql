alter system set work_mem = '4MB';

alter system set shared_buffers = '128MB';

alter system set  random_page_cost = "4" ;
select pg_reload_conf();

drop index akas_id_region_idx ;
drop INDEX idx_titleGenre_title_id_genre_id ;
drop index title_hash;
