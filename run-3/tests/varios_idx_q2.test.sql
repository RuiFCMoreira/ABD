-- 2
create index title_type_idx on title (title_type);
create index title_hash on title using hash (id);
create index userHistory_title_id on userHistory (title_id);
create index title_id_primary_title on title (id,primary_title);