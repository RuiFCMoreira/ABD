-- * 
alter system set  effective_cache_size = "5.7GB" ;
select pg_reload_conf();