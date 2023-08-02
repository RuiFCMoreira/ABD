-- 1
alter system set work_mem = '20MB';

alter system set shared_buffers = '2GB';

alter system set random_page_cost = "1.2" ;
select pg_reload_conf();

create index akas_id_region_idx on titleAkas (title_id, region);
CREATE INDEX idx_titleGenre_title_id_genre_id ON titleGenre (title_id, genre_id);
create index title_hash on title using hash (id);