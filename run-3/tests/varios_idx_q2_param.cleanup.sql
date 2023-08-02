drop index title_type_idx ;
drop index title_hash ;
drop index userHistory_title_id ;
drop index title_id_primary_title ;

alter system set work_mem = '4MB';

alter system set shared_buffers = '128MB';

alter system set  random_page_cost = "4" ;
select pg_reload_conf();