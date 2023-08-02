drop index tpc_title_name ;
drop index title_type_idx ;
drop index tp_title_id_idx ;

alter system set work_mem = '4MB';

alter system set shared_buffers = '128MB';

alter system set random_page_cost = "4" ;
select pg_reload_conf();