# Bitemporal Metadata

Bitemporal tables need to store information about constraints
that are specific to bitemporal tables. This implementation has
chosen to leverage the existing catalogue to store this information.
As there are no specific bitemporal support in postgres the
information must be encoded into existing catalogue entry types.
This encoded data must be read and parsed to return the actionable
information.

This meta data deals with three constraints: Foreign Key, Primary Key and
Unique.

The other constraints are not considered to be different in the cause of
bitemporal tables vs normal tables. The not null constraint is the same
on both tables. Triggers, Exclusion and Check constraints
will need to be written as bitemporal aware.
There is no support for them in these functions.


## Create Constraints

There are three functions to create the necessary clauses for a create table
or an alter table statement to add the particular bitemporal constraint.

These constraints are not enforced by postgres itself. Therefore it is
necessary to store information about the constraints in the pg catalog. These
entries are encoded in the source column of specially named check constraints.
The check constraint is suppose to always be true
and contains a string with @ at the beginning and end to delimit the
information about the bitemporal constraint.

### PK

The Primary Key supports a single column which is encoded as the columns name
delimited by @.

This creates a clause for recording a single column as a primary key of a
bitemporal table.

```sql
    select bitemporal_internal.pk_constraint('id')
```

### FK

The Foregn Key supports a single column to column FK. The tuple of column,
foreign table and foreign column are encoded similar to this
'@ local_column -> remote_table(remote_column) @'

Thie creates a clause for recording a foreign key relationship between
a single column in the local table and a single table in a foreign table.
This takes three arguments. The first is the local column, the second is the
foriegn table and the third is the foriegn table column.

```sql
    select bitemporal_internal.fk_constraint('user_id', 'users', 'user_id')
```


### Unique

The Unique constraint is enforced by postgres by reencoding a bitemporal
unique constraint as an exclude constraint with the unique column and
the bitemporal time ranges.

This creates a constraint to maintain a unique column of values across the
bitemporal dimensions.

```sql
    select string_agg(a, ',\n') from
        bitemporal_internal.unique_constraint('username') as s(a)
```


### Alter helper

This methods constructs the alter table statement given the output of one of
the functions above.

```sql
    execute bitemporal_internal.add_constraint('users', bitemporal_internal.pk_constraint('username') );
```


## Search for Bitemporal Constraints

There are two functions to find bitemporal Primary Key and bitemporal
Foriegn Key constraints. These function decode the information embeded in the catalog.


### Primary Key.

The functions finds the bitemporal primary key for the given table. The name
of the column is returned as a text value. Only one column is supported.

```sql
  select bitemporal_internal.find_pk('schema.table_name')
```

### Foreign Key

The function finds all the bitemporal Foreign Key constraints for a given
table. The fucntion returns the complete set of all Foreign Keys for the
given table.

There is a special composite type in the
form of a table *bitemporal\_internal.fk\_constraint\_type*.

```sql
    create table if not exists bitemporal_internal.fk_constraint_type (
       conname name
      , src_column  name
      , fk_table text
      , fk_column name
    );
```

The connname exists to help debug is not required to determine the FK
relationship. The type also only supports foregn keys of one column.

```sql
    select * from bitemporal_internal.find_fk('schema.table_name')
```




# Examples

The two main use cases are adding constraints at table creation time or after
table exists.


To add a unique_constraint to a an existing table *users* for the column
*username* you would do the following. Using the ```unique_constraint```
function to create a valid constraint clause the ```add_constraint``` function
place the clause in an alter statement that can be executed.

```sql
DO $do$
DECLARE
  rc text;
BEGIN
    select string_agg(a, ';') into rc from
        ( select bitemporal_internal.add_constraint('users', aa) from
                bitemporal_internal.unique_constraint('username') as s(aa)
        ) as t(a);
    execute rc;
END;
$do$;
```

The ```execute_add_constraint``` function will dynamicalyy execute the alter statement.
```sql
    select bitemporal_internal.execute_add_constraint('users', a) from
           bitemporal_internal.unique_constraint('username') as s(a) ;
```



If you which is create a primary key clause to add to a create table you can get the
clause by the following.

```sql
    select bitemporal_internal.pk_constraint('id')
```


If you wish to create a table with a FK cluase at the same time you may do something like
the following.


```sql
    DO $do$ BEGIN
      execute format($$create table groups ( group_id serial,
           group_name text, user_id int,
           % ); $$,
          bitemporal_internal.fk_constraint('user_id', 'users', 'user_id') );
    END; $do$;
```


## Example of how Catalog is Used


### calls to functions with clause output
### resulting create table
### select conname, contype, consrc from pg_constraints where table rel

