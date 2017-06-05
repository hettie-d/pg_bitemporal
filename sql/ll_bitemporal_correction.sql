CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction(
    p_table text,
    p_list_of_fields text,
    p_list_of_values text,
    p_search_fields text,
    p_search_values text,
    p_effective temporal_relationships.timeperiod,
    p_now temporal_relationships.time_endpoint )
  RETURNS integer AS
$BODY$
DECLARE
  v_rowcount INTEGER:=0;
  v_list_of_fields_to_insert text;
  v_table_attr text[];
  v_now temporal_relationships.time_endpoint:=p_now ;-- for compatiability with the previous version
<<<<<<< HEAD
 -- v_last_asserted_start temporal_relationships.time_endpoint;
=======
>>>>>>> scalegenius/master
BEGIN
 v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(p_table);
 IF  array_length(v_table_attr,1)=0
      THEN RAISE EXCEPTION 'Empty list of fields for a table: %', p_table; 
  RETURN v_rowcount;
 END IF;
 v_list_of_fields_to_insert:= array_to_string(v_table_attr, ',','');

 EXECUTE format($u$ UPDATE %s SET asserted = temporal_relationships.timeperiod_range(lower(asserted), %L, '[)')
                    WHERE ( %s )=( %s ) AND effective = %L
                          AND upper(asserted)='infinity' $u$  --end assertion period for the old record(s)
          , p_table
          , v_now
          , p_search_fields
          , p_search_values
          , p_effective);

 EXECUTE format($i$INSERT INTO %s ( %s, effective, asserted )
                SELECT %s ,effective, temporal_relationships.timeperiod_range(upper(asserted), 'infinity', '[)')
                  FROM %s WHERE ( %s )=( %s ) AND effective = %L
                          AND upper(asserted)= %L $i$  --insert new assertion rage with old values 
          , p_table
          , v_list_of_fields_to_insert
          , v_list_of_fields_to_insert
          , p_table
          , p_search_fields
          , p_search_values
          , p_effective
          , v_now
);

    EXECUTE format($uu$UPDATE %s SET ( %s ) = ( %s ) WHERE ( %s ) = ( %s )
                           AND effective = %L
                           AND upper(asserted)='infinity'
                           RETURNING * $uu$  --update new assertion rage with new values
          , p_table
          , p_list_of_fields
          , p_list_of_values
          , p_search_fields
          , p_search_values
          , p_effective
     ) ;
 GET DIAGNOSTICS v_rowcount:=ROW_COUNT; 
 RETURN v_rowcount;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
  
  CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction(
    p_table text,
    p_list_of_fields text,
    p_list_of_values text,
    p_search_fields text,
    p_search_values text,
    p_effective temporal_relationships.timeperiod)
  RETURNS integer AS
  $BODY$
  declare v_rowcount int;
  begin
   select * into v_rowcount from  bitemporal_internal.ll_bitemporal_correction(
    p_table ,
    p_list_of_fields ,
    p_list_of_values ,
    p_search_fields ,
    p_search_values,
    p_effective ,
    clock_timestamp() );
    return v_rowcount;
    END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
  
  