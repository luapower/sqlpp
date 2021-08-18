
## `local sqlpp = require'sqlpp'`

SQL Preprocessor, postprocessor and command API.

Preprocessor features:

 * `#if #elif #else #endif` conditionals
 * `$foo` text substitutions
 * `$foo(args...)` macro substitutions
 * `?` and `:foo` quoted-value substitutions
 * `??` and `::foo` quoted-name substitutions
 * `{foo}` unquoted substitutions
 * symbol substitutions (for encoding `null` and `default`)
 * removing double-dash comments (for [mysql_client])

## API Summary
----------------------------------------------- ------------------------------
__preprocessor__
`sqlpp.new() -> spp`                            create a preprocessor instance
`spp.query(sql, ...) -> sql, names`             preprocess a query
`spp.params(sql, [t]) -> sql, names`            substitute named params
`spp.args(sql, ...) -> sql`                     substitute positional args
`spp.string(s) -> s`                            format string literal
`spp.number(x) -> s`                            format number literal
`spp.boolean(v) -> s`                           format boolean literal
`spp.name(s) -> s`                              format name: `'foo.bar'` -> `'`foo`.`bar`'`
`spp.val(v[, field]) -> s`                      format any value
`spp.rows(rows[, indent]) -> s`                 format `{{a,b},{c,d}}` as `'(a, b), (c, d)'`
`spp.tsv_rows(opt, s) -> rows`                  convert a tab-separated list to a list of rows
`spp.tsv(opt, s) -> s`                          format a tab-separated list
`spp.keyword.KEYWORD -> symbol`                 get a symbol for a keyword
`spp.keywords[SYMBOL] = keyword`                set a keyword for a symbol
`spp.subst'NAME text...'`                       create a substitution for `$NAME`
`function spp.macro.NAME(...) end`              create a macro to be used as `$NAME(...)`
`spp.has_ddl(sql) -> true|false`                check if an expanded query has DDL commands in it
__post-processor__
`spp.groups(col, rows|groups) -> groups`        group rows
__command API__
`spp.connect(options) -> cmd`                   make a command API based on a low-level API
__query execution__
`cmd:query([opt], sql, ...) -> rows`            query
`cmd:first_row([opt], sql, ...) -> rows`        query and return the first row
`cmd:each_row([opt], sql, ...) -> iter`         query and iterate rows
`cmd:each_row_vals([opt], sql, ...)-> iter`     query and iterate rows unpacked
`cmd:each_group(col, [opt], sql, ...) -> iter`  query, group by col and iterate groups
`cmd:atomic(fn, ...) -> ...`                    call a function inside a transaction
__DDL commands__
`cmd:table_def('[schema.]table') -> t`          get table definition (cached)
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
__module system__
`function sqlpp.package.NAME(spp) end`          extend the preprocessor with a module
`spp.require(name)`                             load a module into the preprocessor instance
__modules__
`spp.require'mysql'`                            MySQL module
`spp.require'mysql_domains'`                    common type domains for MySQL
----------------------------------------------- ------------------------------

## Preprocessor

### `sqlpp.new() -> spp`

Create a preprocessor instance. Modules can be loaded into the instance
with `spp.require()`.

The preprocessor doesn't work all by itself, it needs a database-specific
module to implement the particulars of your database engine. For instance
`spp.string()` is a stub and for eg. MySQL is implemented in the module
`sqlpp_mysql`.

### `spp.query(sql, ...) -> sql, names`

Preprocess a SQL query, including hashtag conditionals, macro substitutions,
quoted and unquoted param substitutions, symbol substitutions and removing
comments.

Named args (called params, i.e. `:foo` and `::foo`) are looked up in vararg#1
if it's a table but positional args (called args, i.e. `?` and `??`) are
looked up in `...`, so you're not allowed to mix params and args in the same
query otherwise vararg#1 would be used both for params and the first arg.

Notes on value expansion:

  * no expansion is done inside string literals so it's safe to do multiple
  preproceessor passes (i.e. preprocess partially-preprocessed queries).

  * list values of form `{foo, bar}` expand to `foo, bar`, which is mostly
  useful for `in (?)` expressions (also because an empty list `{}` expands
  to `null` instead of empty string which would result in `in ()` which is
  invalid syntax in MySQL).

  * never use dots in schema names, table names or column names,
  or `pp.name()` won't work (other SQL tools won't work either).

  * avoid using multi-line comments except for optimizer hints because
  the preprocessor does not currently know to avoid parsing inside them
  as it does with strings.

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
instances to be loaded with `spp.require()`.

### `spp.require(name)`

Load a module into the preprocessor instance.

## Command API

### `spp.connect(options) -> cmd`

Make a command API based on a low-level query exec API containing:

----------------------------------- ------------------------------------------
`cmd:rawconnect()`                  one-time method to establish a connection
`cmd:rawexec(sql, opt) -> rows`     query function
`cmd:rawagain(opt) -> rows`         for queries with multiple result sets
----------------------------------- ------------------------------------------

The returned command API inherits from `spp.command` which is where
sqlpp modules publish their commands.

## Modules

### `require'sqlpp_mysql'` <br> `spp.require'mysql'`

MySQL module that extends a sqlpp instance with MySQL-specific quoting,
DDL macros, DDL commands and MDL commands.

#### Quoting

MySQL-specific quoting of values: `inf` and `nan` expand to `null`,
booleans expand to `1` and `0`.

### `spp.require'mysql_domains'`

MySQL module with common type domains. Provided separately as those are
non-standard naming conventions.

