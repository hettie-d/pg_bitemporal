--select bitemporal_internal.ll_generate_bitemp_for_schema('capture_messages_core_reporting')

create or replace function bitemporal_internal.ll_generate_bitemp_for_schema(p_schema_name text)
returns text as
$BODY$
declare
v_rec record;
v_rec2 record;
v_table_definition text;
v_business_key text;
v_business_key_def int2[];
v_all_tables text:=' ';
begin
for v_rec in (select c.relname, lower(c.relname) as stg_name,
c.oid from pg_class c   
JOIN pg_namespace n ON n.oid = c.relnamespace and relkind='r' 
and n.nspname=p_schema_name
order by 1)
loop
v_table_definition:= format(
$text$select * from 
bitemporal_internal.ll_create_bitemporal_table ('%s_bt',
%L,
'$text$, 
p_schema_name,
v_rec.stg_name);
select conkey into v_business_key_def from pg_constraint where conrelid=v_rec.oid
and contype='p';
--raise notice '%', v_business_key_def;
v_business_key:=NULL;
for v_rec2 in (select ordinal_position,
	             column_name::text as column_name,
				       data_type::text||		 
              case when character_maximum_length is not null 
                 then '('||character_maximum_length::text||')'
                 else ''
                 end ||
                 case data_type when 'numeric'
                 then '('||numeric_precision::text||','||numeric_scale::text|| ')'
                 else ''
                 end
                 as data_type,
              case is_nullable 
                when 'NO' then 'NOT NULL'
                else ''
                end as nullable
	    from information_schema.columns 
      where table_schema=p_schema_name
            and table_name=v_rec.stg_name    
order by ordinal_position)
loop
if v_rec2.ordinal_position>1 then 
v_table_definition:=
v_table_definition||',';
end if;
v_table_definition:=
v_table_definition||format($text$%s %s 
$text$,
v_rec2.column_name, 
v_rec2.data_type); 
if v_rec2.ordinal_position = any (v_business_key_def)
then if  v_business_key is null
then v_business_key:=v_rec2.column_name ;else 
   v_business_key:=v_business_key||','||v_rec2.column_name;
   end if;
 end if;  
 
end loop;
v_table_definition:= v_table_definition|| format(
$text$', 
%L
);

$text$, 
v_business_key);

v_all_tables:=v_all_tables|| v_table_definition;
end loop;
return v_all_tables;
end;
$BODY$
  LANGUAGE plpgsql;