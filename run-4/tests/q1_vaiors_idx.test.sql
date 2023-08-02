-- 1
create index akas_id_region_idx on titleAkas (title_id, region);
CREATE INDEX idx_titleGenre_title_id_genre_id ON titleGenre (title_id, genre_id);
create index userHistory_title_id on userHistory (title_id);

alter system set max_parallel_workers_per_gather = 4;
select pg_reload_conf();