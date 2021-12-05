
## `local sqlpp = require'sqlpp'`

SQLpp is an SQL preprocessor, result-set processor and SQL generator.
Written as a generic wrapper over your favorite SQL connector library,
its purpose is to give SQL more power, unlike ORMs which take away even
the one it has. So definitely not for OOP-heads. If you like SQL though,
sqlpp will help you make dynamic queries, generate update queries for you
from structored data, extract database schema into a structured format
and even generate DDL SQL from [schema] diffs so you can automate your
migrations.

### Preprocessor features

 * `#if #elif #else #endif` conditionals
 * `$foo` text substitutions
 * `$foo(args...)` macro substitutions
 * `?` and `:foo` quoted-value substitutions
 * `??` and `::foo` quoted-name substitutions
 * `{foo}` unquoted substitutions
 * symbol substitutions (for encoding `null` and `default`)
 * removing double-dash comments (for [mysql_client])

### Backends & writing your own

Only supports MySQL via [mysql_client] for now. Writing a backend for your
favorite RDBMS is easy though. At the minimum you have to show sqlpp how to
connect to your engine and how to quote strings, and if you want schema diffs
you have to write the queries to extract metadata from information tables.
Use `sqlpp_mysql.lua` as a reference when writing your backend.

## API Summary
----------------------------------------------- ------------------------------
`sqlpp.new() -> spp`                            create a preprocessor instance
`spp.connect(options) -> cmd`                   connect to a database
`spp.use(rawconn) -> cmd`                       use an existing connection
__SQL formatting__
`cmd:sqlname(s) -> s`                           format name: `'foo.bar'` -> `'`foo`.`bar`'`
`cmd:esc(s) -> s`                               escape a string to be used inside SQL string literals
`cmd:sqlval(v[, field]) -> s`                   format any value
`cmd:sqlrows(rows[, indent]) -> s`              format `{{a,b},{c,d}}` as `'(a, b), (c, d)'`
`spp.tsv_rows(opt, s) -> rows`                  convert a tab-separated list to a list of rows
`cmd:sqltsv(opt, s) -> s`                       format a tab-separated list
`cmd:sqldiff(o, opt)`                           format a [schema] diff object
__SQL preprocessing__
`cmd:sqlquery(sql, ...) -> sql, names`          preprocess a query
`cmd:sqlprepare(sql, ...) -> sql, names`        preprocess a query leaving `?` placeholders
`cmd:sqlparams(sql, [t]) -> sql, names`         substitute named params
`cmd:sqlargs(sql, ...) -> sql`                  substitute positional args
__Query execution__
`cmd:query([opt], sql, ...) -> rows`            query with preprocessing
`cmd:first_row([opt], sql, ...) -> rows`        query and return the first row
`cmd:each_row([opt], sql, ...) -> iter`         query and iterate rows
`cmd:each_row_vals([opt], sql, ...)-> iter`     query and iterate rows unpacked
`cmd:each_group(col, [opt], sql, ...) -> iter`  query, group by col and iterate groups
`cmd:prepare([opt], sql, ...) -> stmt`          prepare query
`stmt:exec(...) -> rows`                        execute prepared query
`stmt:first_row(...) -> rows`                   query and return the first row
`stmt:each_row(...) -> iter`                    query and iterate rows
`stmt:each_row_vals(...)-> iter`                query and iterate rows unpacked
`stmt:each_group(col, ...) -> iter`             query, group by col and iterate groups
`cmd:atomic(fn, ...) -> ...`                    call a function inside a transaction
`cmd:has_ddl(sql) -> true|false`                check if an expanded query has DDL commands in it
__Grouping result rowsets__
`spp.groups(col, rows|groups) -> groups`        group rows
`spp.each_group(col, rows|groups) -> iter`      group rows and iterate groups
__DDL commands__
`cmd:table_def(['[DB.]TABLE|*']) -> t`          get table definition (cached)
`cmd:create_database(name)`                     create database
`cmd:drop_table(name)`                          drop table
`cmd:drop_tables('T1 T2 ...')`                  drop multiple tables
`cmd:add_column(tbl, name, type, pos)`          add column
`cmd:rename_column(tbl, oldname, newname)`      rename column
`cmd:drop_column(tbl, col)`                     drop column
`cmd:[re]add_fk(tbl, col, ...)`                 (re)create foreign key
`cmd:[re]add_uk(tbl, col)`                      (re)create unique key
`cmd:[re]add_ix(tbl, col)`                      (re)create index
`cmd:drop_fk(tbl, col)`                         drop foreign key
`cmd:drop_uk(tbl, col)`                         drop unique key
`cmd:drop_ix(tbl, col)`                         drop index
`cmd:[re]add_trigger(name, tbl, on, code)`      (re)create trigger
`cmd:drop_trigger(name, tbl, on)`               drop trigger
`cmd:[re]add_proc(name, args, code)`            (re)create stored procedure
`cmd:drop_proc(name)`                           drop stored procedure
`cmd:[re]add_column_locks(tbl, cols)`           trigger to make columns read-only
`cmd:drop_column_locks(tbl)`                    drop all column locks on a table
__MDL commands__
`cmd:insert_row(tbl, vals, col_map)`            insert a row
`cmd:insert_rows(tbl, rows, col_map, compact)`  insert rows with one query
`cmd:update_row(tbl, vals, col_map, [filter])`  update row
`cmd:delete_row(tbl, vals, col_map, [filter])`  delete row
__Module system__
`function sqlpp.package.NAME(spp) end`          extend the preprocessor with a module
`spp.import(module)`                            import sqlpp module
__Modules__
`require'sqlpp_mysql'`                          load the code for the MySQL module
`spp.import'mysql'`                             load the MySQL module
`spp.import'mysql_domains'`                     load common type domains for MySQL
__Extending the preprocessor__
`spp.keyword.KEYWORD -> symbol`                 get a symbol for a keyword
`spp.keywords[SYMBOL] = keyword`                set a keyword for a symbol
`spp.subst'NAME text...'`                       create a substitution for `$NAME`
`function spp.macro.NAME(...) end`              create a macro to be used as `$NAME(...)`
----------------------------------------------- ------------------------------

## Preprocessor

### `sqlpp.new() -> spp`

Create a preprocessor instance. Modules can be loaded into the instance
with `spp.import()`.

The preprocessor doesn't work all by itself, it needs a database-specific
module to implement the particulars of your database engine. Currently only
the MySQL engine is implemented in the module `sqlpp_mysql`.

### `spp.connect(options) -> cmd` <br> `spp.use(rawconn) -> cmd`

Make a command API based on a low-level query execution API.

### `cmd:sqlquery(sql, ...) -> sql, names`

Preprocess a SQL query, including hashtag conditionals, macro substitutions,
quoted and unquoted param substitutions, symbol substitutions and removing
comments.

Named args (called params, i.e. `:foo` and `::foo`) are looked up in vararg#1
if it's a table but positional args (called args, i.e. `?` and `??`) are
looked up in `...`. Because of this, you're not allowed to mix named args
and positional args in the same query otherwise vararg#1 would be used both
as the table to look-up named args from, and as the first positional arg.

Notes on value expansion:

  * no expansion is done inside string literals so it's safe to do multiple
  preproceessor passes (i.e. preprocess partially-preprocessed queries).

  * list values of form `{foo, bar}` expand to `foo, bar`, which is mostly
  useful for `in (?)` expressions (also because an empty list `{}` expands
  to `null` instead of empty string which would result in `in ()` which is
  invalid syntax in MySQL).

  * never use dots in database names, table names or column names,
  or `sqlname()` won't work (other SQL tools won't work either).

  * avoid using multi-line comments except for optimizer hints because
  the preprocessor does not currently know to avoid parsing inside them
  as it does with string literals (OTOH this allows you to parametrize
  optimizer hints).

### `spp.keyword.KEYWORD -> symbol`

Get a symbol object for a keyword. Symbols are used to encode special SQL
values in parameter values, for instance using `spp.keyword.default`
as a parameter value will be substituted by the keyword `default`.

### `spp.keywords[SYMBOL] = keyword`

Set a keyword to be substituted for a symbol object. Since symbols can be
any Lua objects, you can add a symbol from an external library to be used
directly in SQL parameters. For instance:

```lua
pp.keywords[require'cjson'.null] = 'null'
```

This enables using JSON null values as SQL parameters.

### `spp.subst'NAME text...'`

Create an unquoted text substitution for `$NAME`.

### `function spp.macro.NAME(...) end`

Create a macro to be used as `$NAME(...)`. Param args are expanded before
macros.

## Module system

## `function sqlpp.package.NAME(spp) end`

Extend the preprocessor with a module, available to all preprocessor
instances to be loaded with `spp.import()`.

### `spp.import(name)`

Load a module into the preprocessor instance.

## Modules

### `require'sqlpp_mysql'` <br> `spp.import'mysql'`

MySQL module that extends a sqlpp instance with MySQL-specific quoting,
DDL macros, DDL commands and MDL commands.

#### Quoting

MySQL-specific quoting of values: `inf` and `nan` expand to `null`,
booleans expand to `1` and `0`.

### `spp.import'mysql_domains'`

MySQL module with common type domains. Provided separately as those are
non-standard naming conventions.

