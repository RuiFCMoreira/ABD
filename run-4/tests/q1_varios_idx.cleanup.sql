drop index akas_id_region_idx;
drop INDEX idx_titleGenre_title_id_genre_id;
drop index userHistory_title_id;

alter system set max_parallel_workers_per_gather = 2;
select pg_reload_conf();