-- for rateTitle
create index title_id_hash_idx on userHistory (title_id);

-- for getTitleFromType
create index title_type_title_idx on title (title_type);
create index start_year_title_idx on title (start_year);

-- for getTitleFromPopular
create index last_seen_uh on userHistory (last_seen);
