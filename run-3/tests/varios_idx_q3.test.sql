-- 3
create index tpc_title_name on titlePrincipalsCharacters (title_id, name_id);
create index title_type_idx on title (title_type);
create index tp_title_id_idx on titlePrincipals using hash (title_id);