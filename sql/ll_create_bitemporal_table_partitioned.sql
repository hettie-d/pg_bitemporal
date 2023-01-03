CREATE OR REPLACE FUNCTION bitemporal_internal.ll_create_bitemporal_table_partitioned(
	p_schema text,
	p_table text,
	p_table_definition text,
	p_business_key text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
 AS $BODY$
DECLARE
v_business_key_name text;
v_business_key_gist text;
v_serial_key_name text;
v_serial_key text;
v_pk_constraint_name text;
v_table_definition text;
v_error text;
v_business_key_array text[];
i int;
v_partition_clause text;
v_sql text;
BEGIN
v_serial_key :=p_table||'_key';
v_serial_key_name :=v_serial_key ||' serial';
v_pk_constraint_name:= p_table||'_pk';
v_table_definition :=replace (p_table_definition, ' serial', ' integer');
v_business_key_array :=string_to_array(p_business_key, ',');
v_partition_clause := ' partition by range('||p_business_key||')';
--EXECUTE
v_sql :=format($create$
CREATE TABLE %s.%s (
                 %s
                 ,%s
                 ,effective temporal_relationships.timeperiod NOT NULL
                 ,asserted temporal_relationships.timeperiod  NOT NULL
                 ,row_created_at timestamptz NOT NULL DEFAULT now()
                 ,CONSTRAINT %s PRIMARY KEY (%s,%s)
                     ) %s
                 $create$
                 ,p_schema
                 ,p_table
                 ,v_serial_key_name
                 ,v_table_definition
                  ,v_pk_constraint_name
                  ,v_serial_key
			      ,p_business_key
                 , v_partition_clause
                 ) ;
  raise notice '%', v_sql;
  execute V_sql;
 i:=1;
 while v_business_key_array[i] is not null loop
 execute   format($alter$
    ALTER TABLE %s.%s ALTER %s SET NOT NULL
                 $alter$
                 ,p_schema
                 ,p_table
                 ,v_business_key_array[i]
                 ) ;
     i:=i+1;
     end loop;
 RETURN ('true');
 EXCEPTION WHEN OTHERS THEN
GET STACKED DIAGNOSTICS v_error = MESSAGE_TEXT;
raise notice '%', v_error;
RETURN ('false');
END;
$BODY$;
