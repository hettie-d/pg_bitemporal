CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_split_effective(p_schema_name text
,p_table_name TEXT
,p_search_fields TEXT  -- search fields
,p_search_values TEXT  --  search values
,p_effective_split temporal_relationships.time_endpoint
,p_asserted temporal_relationships.timeperiod  -- assertion for the update
) 
RETURNS INTEGER
AS
$BODY$
DECLARE
v_rowcount INTEGER:=0;
v_list_of_fields_to_insert text:=' ';
v_list_of_fields_to_insert_excl_effective text;
v_table_attr text[];
v_serial_key text:=p_table_name||'_key';
v_table text:=p_schema_name||'.'||p_table_name;
v_keys_old int[];
v_keys int[];
v_now timestamptz:=now();-- so that we can reference this time
BEGIN 
 /*IF lower(p_asserted)<v_now::date --should we allow this precision?...
    OR upper(p_asserted)< 'infinity'
 THEN RAISE EXCEPTION'Asserted interval starts in the past or has a finite end: %', p_asserted
  ; 
  RETURN v_rowcount;
 END IF;  
*/
v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(v_table);
IF  array_length(v_table_attr,1)=0
      THEN RAISE EXCEPTION 'Empty list of fields for a table: %', v_table; 
  RETURN v_rowcount;
 END IF;
v_list_of_fields_to_insert_excl_effective:= array_to_string(v_table_attr, ',','');
v_list_of_fields_to_insert:= v_list_of_fields_to_insert_excl_effective||',effective,asserted';

EXECUTE format($u$ WITH updt AS (UPDATE %s SET effective =
            temporal_relationships.timeperiod(lower(effective), %L::temporal_relationships.time_endpoint)
                    WHERE ( %s )=( %s ) 
                    AND   %L::timestamptz<@effective
                    AND %L::temporal_relationships.time_endpoint!=lower(effective)
                    AND %L<@ asserted  returning %s )
                                      INSERT INTO %s ( %s )                                    
                                        SELECT %s,
                                           temporal_relationships.timeperiod(%L::temporal_relationships.time_endpoint, upper(effective)),
                                           asserted
                                       FROM %s WHERE %s IN (SELECT %s FROM updt)                                  
                                      $u$  
          , v_table
          , p_effective_split
          , p_search_fields
          , p_search_values
          , p_effective_split
          , p_effective_split
	       ,p_asserted
          , v_serial_key
          , v_table
          , v_list_of_fields_to_insert
          , v_list_of_fields_to_insert_excl_effective
          , p_effective_split
          , v_table
          , v_serial_key
          , v_serial_key) ;

          
GET DIAGNOSTICS v_rowcount:=ROW_COUNT;  

RETURN v_rowcount;
END;    
$BODY$ LANGUAGE plpgsql;

