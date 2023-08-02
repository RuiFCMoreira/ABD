alter system set  effective_cache_size = "4GB" ;
select pg_reload_conf();