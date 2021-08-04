
--SQL preprocessor.
--Written by Cosmin Apreutesei. Public Domain.

local glue = require'glue'

local fmt = string.format
local add = table.insert
local concat = table.concat

local assertf = glue.assert

local M = {package = {}}

local esc = {
	['\0' ]  = '\\0',
	['\b' ]  = '\\b',
	['\n' ]  = '\\n',
	['\r' ]  = '\\r',
	['\t' ]  = '\\t',
	['\26']  = '\\Z',
	['\\' ]  = '\\\\',
	['\'' ]  = '\\\'',
	['\"' ]  = '\\"',
}

function M.new()

	local pp = {package = {}, command = {}}

	--conditional compilation -------------------------------------------------

	--Process #if #elif #else #endif conditionals.
	--Also normalize newlines and remove single-line comments which the mysql
	--client protocol cannot parse. Multiline comments are not removed since
	--they can be used for optimizer hints.
	local globals_mt = {__index = _G}
	local function parse_expr(s, params)
		local f = assert(loadstring('return '..s))
		params = glue.update({}, params) --copy it so we alter it
		setmetatable(params, globals_mt)
		setfenv(f, params)
		return f()
	end
	local function pp_ifs(sql, params)
		local t = {}
		local state = {active = true}
		local states = {state}
		local level = 1
		for line in glue.lines(sql) do
			local s, expr = line:match'^%s*#([%w_]+)(.*)'
			if s == 'if' then
				level = level + 1
				if state.active then
					local active = parse_expr(expr, params) and true or false
					state = {active = active, activated = active}
				else
					state = {active = false, activated = true}
				end
				states[level] = state
			elseif s == 'else' then
				assert(level > 1, '#else without #if')
				assert(not state.done, '#else after #else')
				if not state.activated then
					state.active = true
					state.activated = true
				else
					state.active = false
				end
				state.done = true
			elseif s == 'elif' then
				assert(level > 1, '#elif without #if')
				assert(not state.done, '#elif after #else')
				if not state.activated and parse_expr(expr, params) then
					state.active = true
					state.activated = true
				else
					state.active = false
				end
			elseif s == 'endif' then
				assert(level > 1, '#endif without #if')
				states[level] = nil
				level = level - 1
				state = states[level]
			elseif state.active then
				line = line:gsub('%-%-.*', '') --remove `-- ...` comments
				line = line:gsub('#.*', '') -- remove `# ...` comments
				if glue.trim(line) ~= '' then
					add(t, line)
				end
			end
		end
		assert(level == 1, '#endif missing')
		return concat(t, '\n')
	end

	--quoting -----------------------------------------------------------------

	pp.keywords = {} --{sym->k}
	pp.keyword = {}
	setmetatable(pp.keyword, {
		__index = function(t, k)
			local sym = {}
			t[k] = sym
			pp.keywords[sym] = k
			return sym
		end,
	})

	local _= pp.keyword.null
	local _= pp.keyword.default

	function pp.string(s) --stub
		return s:gsub('[%z\b\n\r\t\26\\\'"]', esc)
	end

	function pp.number(x) --stub
		return fmt('%0.17g', x) --max precision, min length.
	end

	function pp.boolean(v) --stub
		return tostring(v)
	end

	function pp.name(v)
		assert(v, 'sql name missing')
		if v:sub(1, 1) == '`' then
			return v
		end
		return '`'..v..'`'
	end

	function pp.value(v)
		if v == nil then
			return 'null'
		elseif type(v) == 'number' then
			return pp.number(v)
		elseif type(v) == 'string' then
			return fmt("'%s'", pp.string(v))
		elseif type(v) == 'boolean' then
			return pp.boolean(v)
		elseif type(v) == 'table' then
			if #v > 0 then --list: for use in `in (?)`
				local t = {}
				for i,v in ipairs(v) do
					t[i] = pp.value(v)
				end
				return concat(t, ', ')
			else --empty list: good for 'in (?)' but NOT GOOD for `not in (?)` !!!
				return 'null'
			end
		elseif pp.keywords[v] then
			return pp.keywords[v]
		else
			return nil, 'invalid value ' .. v
		end
	end

	function pp.rows(rows, t) --{{v1,...},...} -> '(v1,...),\n (v2,...)'
		local max_sizes = {}
		local pad_dirs = {}
		local srows = {}
		for ri,row in ipairs(rows) do
			local srow = {}
			srows[ri] = srow
			local n = t and t.n or #row
			for ci = 1, n do
				local s = row[ci]
				if type(s) == 'function' then --self-generating value.
					s = s()
				else
					pad_dirs[ci] = type(s) == 'number' and 'left' or 'right'
					local convert = t and t[ci] or pp.value
					if type(convert) == 'function' then --col transform.
						s = convert(s)
					else --constant.
						s = convert
					end
				end
				srow[ci] = s
				max_sizes[ci] = math.max(max_sizes[ci] or 0, #s)
			end
		end
		local dt = {}
		local prefix = (t and t.indent or '')..'('
		for ri,row in ipairs(srows) do
			local t = {}
			for ci,s in ipairs(row) do
				t[ci] = glue.pad(s, max_sizes[ci], ' ', pad_dirs[ci])
			end
			dt[ri] = prefix..concat(t, ', ')..')'
		end
		return concat(dt, ',\n')
	end

	function pp.tsv_rows(t, s) --{n=3|cols='3 1 2', transform1, ...}
		s = glue.trim(s)
		local cols
		if t.cols then
			cols = {}
			for s in t.cols:gmatch'[^%s]+' do
				cols[#cols+1] = assert(tonumber(s))
			end
		end
		local n = t.n
		if not n then
			if cols then
				local cols = glue.extend({}, cols)
				table.sort(cols)
				n = cols[#cols]
			else
				local s = s:match'^[^\r\n]+'
				if s then
					n = 1
					for _ in s:gmatch'\t' do
						n = n + 1
					end
				else
					n = 1
				end
			end
		end
		cols = cols and glue.index(cols) --{3, 1, 2} -> {[3]->1, [1]->2, [2]->3}
		local patt = '^'..('(.-)\t'):rep(n-1)..'(.*)'
		local function transform_line(row, ...)
			for i=1,n do
				local di = not cols and i or cols[i]
				if di then
					local s = select(i,...)
					local transform_val = t[i]
					if transform_val then
						s = transform_val(s)
					end
					row[di] = s
				end
			end
		end
		local rows = {}
		local ri = 1
		for s in glue.lines(s) do
			local row = {}
			rows[ri] = row
			transform_line(row, s:match(patt))
			ri = ri + 1
		end
		return rows
	end

	function pp.tsv(t, s)
		return pp.rows(pp.tsv_rows(t, s), t.rows)
	end

	local function named_params(sql, t)
		local names = {}
		local sql = sql:gsub('::([%w_]+)', function(k) -- ::col, ::table, etc.
			add(names, k)
			local v, err = pp.name(t[k])
			return assertf(v, 'param %s: %s\n%s', k, err, sql)
		end)
		local sql = sql:gsub(':([%w_][%w_%:]*)', function(k) -- :foo, :foo:old, etc.
			add(names, k)
			local v, err = pp.value(t[k])
			return assertf(v, 'param %s: %s\n%s', k, err, sql)
		end)
		return sql, names
	end

	local function indexed_params(sql, t)
		local i = 0
		return (sql:gsub('%?%?', function()
			i = i + 1
			local v, err = pp.name(t[i])
			return assertf(v, 'param %d: %s\n%s', i, err, sql)
		end):gsub('%?', function()
			i = i + 1
			local v, err = pp.value(t[i])
			return assertf(v, 'param %d: %s\n%s', i, err, sql)
		end))
	end

	function pp.params(sql, t)
		local sql = indexed_params(sql, t)
		return named_params(sql, t)
	end

	--macros ------------------------------------------------------------------

	local defines = {}

	function pp.subst(def) --'name type'
		local name, val = def:match'([%w_]+)%s+(.*)'
		assertf(not defines[name], 'macro already defined: %s', name)
		defines[name] = val
	end

	pp.macro = {}

	local function macro_arg(arg, t)
		local k = arg:match'^:([%w_][%w_%:]*)'
		if k then --unparsed param expansion.
			return t[k]
		else --parsed param expansion.
			return pp.params(arg, t)
		end
	end

	local function macro_subst(name, args, t)
		local macro = assertf(pp.macro[name], 'undefined macro: $%s()', name)
		args = args:sub(2,-2)..','
		local dt = {}
		for arg in args:gmatch'([^,]+)' do
			arg = glue.trim(arg)
			dt[#dt+1] = macro_arg(arg, t) --expand params in macro args *unquoted*!
		end
		return macro(unpack(dt))
	end

	--preprocessor ------------------------------------------------------------

	--TODO: separate multiple queries based on delim and support `delimiter` command.

	function pp.query(sql, t, delim)
		t = t or glue.empty

		local sql = pp_ifs(sql, t) --#if ... #endif

		--we do macro expansion in two steps because we can't expand a query
		--after it has been expanded once because we would parse inside string
		--literals; also because we can't yield from gsub and we want to
		--support macros that call query() which yields.

		--step 1: collect all macros and replace them with a marker that
		--string literals can't contain.

		local repl  = {m = {}, d = {}, v = {}}

		local macros = {}
		local function collect_macro(name, args)
			add(macros, name)
			add(macros, args)
			return '\0m' --marker
		end
		sql = sql:gsub('$([%w_]+)(%b())', collect_macro) --$foo(arg1,...)

		for i = 1, #macros, 2 do
			local name, args = macros[i], macros[i+1]
			add(repl.m, macro_subst(name, args, t) or '')
		end

		local function collect_define(name)
			add(repl.d, assertf(defines[name], '$%s is undefined', name))
			return '\0d' --marker
		end
		sql = sql:gsub('$([%w_]+)', collect_define) --$foo

		local function collect_verbatim(name)
			add(repl.v, assertf(t[name], '{%s} is missing'))
			return '\0v' --marker
		end
		local sql = glue.subst(sql, collect_verbatim) --{foo}

		--step 2: expand params.

		local sql, names = pp.params(sql, t) -- ? ?? :foo :foo

		--step 3: expand markers.

		local repln = {m = 0, d = 0, v = 0}
		sql = sql:gsub('%z(.)', function(k)
			local i = repln[k] + 1
			repln[k] = i
			return repl[k][i]
		end)

		return sql, names
	end

	function pp.queries(sql, t, delim)
		local sql = pp.query(sql, t, delim)
		return {sql}
	end

	--module system -----------------------------------------------------------

	function pp.require(pkg)
		for pkg in pkg:gmatch'[^%s]+' do
			if not pp.package[pkg] then
				assertf(M.package[pkg], 'no sqlpp module: %s', pkg)(pp)
				pp.package[pkg] = true
			end
		end
	end

	--commands ----------------------------------------------------------------

	function pp.command_api(run_query)
		local api = {}
		for name,f in pairs(pp.command) do
			api[name] = function(...)
				return f(run_query, ...)
			end
		end
		return api
	end

	return pp
end

--mysql ----------------------------------------------------------------------

function M.package.mysql_quote(pp)

	local quote_number = pp.number
	function pp.number(v)
		if v ~= v or v == 1/0 or v == -1/0 then
			return 'null' --avoid syntax error for what ends up as null anyway.
		end
		return quote_number(v)
	end

	function pp.boolean(v)
		return v and 1 or 0
	end

end

function M.package.mysql_ddl(pp)

	pp.subst'table  create table if not exists'

	local cmd = pp.command

	function cmd.index_exists(q, name)
		return q([[
			select 1 from information_schema.innodb_indexes i
			where i.name = ?
		]], name)[1] ~= nil --single-column result returned as array of values.
	end

	function cmd.trigger_exists(q, name)
		return q([[
			select 1 from information_schema.triggers
			where trigger_name = ?
		]], name)[1] ~= nil
	end

	function cmd.proc_exists(q, name)
		return q([[
			select 1 from information_schema.routines
			where routine_schema = database() and routine_name = ?
		]], name)[1] ~= nil
	end

	function cmd.column_exists(q, tbl, name)
		return q([[
			select 1 from information_schema.columns
			where table_schema = database() and table_name = ? and column_name = ?
		]], tbl, name)[1] ~= nil
	end

	local function cols(s, newsep)
		return s:gsub('%s+', newsep or ',')
	end

	local function dename(s)
		return s:gsub('`', '')
	end

	local function deixcol(s)
		return s
			:gsub(' asc$', ''):gsub(' desc$', '')
			:gsub(' asc ', ''):gsub(' desc ', '')
	end

	local function indexname(type, tbl, col)
		return fmt('%s_%s_%s', type, dename(tbl), dename(cols(col, '_')))
	end
	local function fkname(tbl, col) return indexname('fk', tbl, col) end
	local function ukname(tbl, col) return indexname('uk', tbl, col) end
	local function ixname(tbl, col) return indexname('ix', tbl, deixcol(col)) end

	function pp.macro.fk(tbl, col, ftbl, fcol, ondelete, onupdate)
		ondelete = ondelete or 'restrict'
		onupdate = onupdate or 'cascade'
		local a1 = ondelete ~= 'restrict' and ' on delete '..ondelete or ''
		local a2 = onupdate ~= 'restrict' and ' on update '..onupdate or ''
		return fmt('constraint %s foreign key (%s) references %s (%s)%s%s',
			fkname(tbl, col), cols(col), ftbl, cols(fcol or col), a1, a2)
	end

	function pp.macro.child_fk(tbl, col, ftbl, fcol)
		return pp.macro.fk(tbl, col, ftbl, fcol, 'cascade')
	end

	function pp.macro.uk(tbl, col)
		return fmt('constraint %s unique key (%s)', ukname(tbl, col), cols(col))
	end

	local function ixcols(s)
		return cols(s)
			:gsub(',asc$', ' asc'):gsub(',desc$', ' desc')
			:gsub(',asc,', ' asc,'):gsub(',desc,', ' desc,')
	end

	function pp.macro.ix(tbl, col)
		return fmt('index %s (%s)', ixname(tbl, col), ixcols(col))
	end

	--commands

	function cmd.create_database(q, name)
		return q([[
create database if not exists ??
	character set utf8mb4
	collate utf8mb4_unicode_ci]], pp.name(name))
	end

	function cmd.add_fk(q, tbl, col, ftbl, ...)
		if cmd.index_exists(q, fkname(tbl, col)) then return end
		return q('alter table ?? add ' ..
			pp.macro.fk(pp.name(tbl), col, pp.name(ftbl), ...), tbl)
	end

	function cmd.add_uk(q, tbl, col)
		if cmd.index_exists(q, ukname(tbl, col)) then return end
		return q('alter table ?? add ' .. pp.macro.uk(pp.name(tbl), col), tbl)
	end

	function cmd.add_ix(q, tbl, col)
		if cmd.index_exists(q, ixname(tbl, col)) then return end
		return q('alter table ?? add ' .. pp.macro.ix(pp.name(tbl), col), tbl)
	end

	pp.allow_drop = false

	local function drop_index(q, type, tbl, col)
		local name = indexname(type, tbl, col)
		if not pp.allow_drop then return end
		if not cmd.index_exists(q, name) then return end
		local s =
			   type == 'fk' and 'foreign key'
			or type == 'uk' and 'unique key'
			or type == 'ix' and 'index'
			or assert(false)
		return q(fmt('alter table ?? drop %s %s', s, name), tbl)
	end

	function cmd.drop_fk(q, tbl, col) return drop_index(q, 'fk', tbl, col) end
	function cmd.drop_uk(q, tbl, col) return drop_index(q, 'uk', tbl, col) end
	function cmd.drop_ix(q, tbl, col) return drop_index(q, 'ix', tbl, col) end

	function cmd.drop_table(q, name)
		if not pp.allow_drop then return end
		return q('drop table if exists ??', name)
	end

	function cmd.drop_tables(q, s)
		local dt = {}
		for name in s:gmatch'[^%s]+' do
			dt[#dt+1] = cmd.drop_table(q, name)
		end
		return dt
	end

	local add_trigger_sql = [[
create trigger ::name {where} on ::table for each row
begin
{code}
end]]

	local function triggername(name, tbl, where)
		local s = where:gsub('([^%s])[^%s]*%s*', '%1')
		return fmt('%s_%s_%s', tbl, s, name)
	end

	function cmd.readd_trigger(q, name, tbl, where, code)
		local name = triggername(name, tbl, where)
		q('lock tables ?? write', tbl)
		q('drop trigger if exists ??', name)
		code = glue.outdent(code, '\t')
		q(add_trigger_sql, {name = name, table = tbl, where = where, code = code})
		q('unlock tables')
	end

	function cmd.add_trigger(q, name, tbl, where, code)
		local name = triggername(name, tbl, where)
		if cmd.trigger_exists(q, name) then return end
		code = glue.outdent(code, '\t')
		return q(add_trigger_sql, {name = name, table = tbl, where = where, code = code})
	end

	function cmd.drop_trigger(q, name, tbl, where)
		local name = triggername(name, tbl, where)
		return q('drop trigger if exists ??', name)
	end

	function cmd.add_proc(q, name, args, code)
		if cmd.proc_exists(q, name) then return end
		code = glue.outdent(code, '\t')
		return q([[
create procedure ::name ({args})
begin
{code}
end]], {name = name, args = args or '', code = code})
	end

	function cmd.readd_proc(q, name, ...)
		if cmd.drop_proc(q, name) then
			cmd.add_proc(q, name, ...)
		end
	end

	function cmd.drop_proc(q, name)
		return q('drop procedure if exists ??', name)
	end

	function cmd.add_column(q, tbl, name, type_pos)
		if cmd.column_exists(q, tbl, name) then return end
		return q(fmt('alter table ?? add column %s %s', name, type_pos), tbl)
	end

	function cmd.rename_column(q, tbl, old_name, new_name)
		if not cmd.column_exists(q, tbl, old_name) then return end
		return q('alter table ?? rename column ?? to ??', tbl, old_name, new_name)
	end

	function cmd.drop_column(q, tbl, name)
		if not pp.allow_drop then return end
		if not cmd.column_exists(q, tbl, old_name) then return end
		return q('alter table ?? remove column ??', tbl, name)
	end

	function cmd.readd_fk(q, tbl, col, ...)
		if cmd.drop_fk(q, tbl, col) then
			return cmd.add_fk(q, tbl, col, ...)
		end
	end

	function cmd.readd_uk(q, tbl, col)
		if cmd.drop_uk(q, tbl, col) then
			return cmd.add_uk(q, tbl, col)
		end
	end

	function cmd.readd_ix(q, tbl, col)
		if cmd.drop_ix(q, tbl, col) then
			return cmd.add_ix(q, tbl, col)
		end
	end

	--column locks

	local function column_locks_code(cols)
		local code = {}
		for col in cols:gmatch'[^%s]+' do
			code[#code+1] = fmt([[
	if new.%s <=> old.%s then
		signal sqlstate '45000' set message_text = 'Read/only column: %s';
	end if;
]], col, col, col)
		end
		return concat(code)
	end

	function cmd.add_column_locks(q, tbl, cols)
		return cmd.add_trigger(q, 'col_locks', tbl, 'before update', column_locks_code(cols))
	end

	function cmd.readd_column_locks(q, tbl, cols)
		return cmd.readd_trigger(q, 'col_locks', tbl, 'before update', column_locks_code(cols))
	end

	function cmd.drop_column_locks(q, tbl)
		return cmd.drop_trigger(q, 'col_locks', tbl, 'before update')
	end

end

function M.package.mysql_domains(pp)
	pp.subst'id       int unsigned'
	pp.subst'pk       int unsigned primary key auto_increment'
	pp.subst'bigid    bigint unsigned'
	pp.subst'bigpk    bigint unsigned primary key auto_increment'
	pp.subst'name     varchar(64)'
	pp.subst'email    varchar(128)'
	pp.subst'hash     varchar(64) character set ascii'
	pp.subst'url      varchar(2048)'
	pp.subst'bool     tinyint not null default 0'
	pp.subst'bool1    tinyint not null default 1'
	pp.subst'atime    timestamp not null default current_timestamp'
	pp.subst'ctime    timestamp not null default current_timestamp'
	pp.subst'mtime    timestamp not null default current_timestamp on update current_timestamp'
	pp.subst'money    decimal(20,6)'
	pp.subst'qty      decimal(20,6)'
	pp.subst'percent  decimal(20,6)'
	pp.subst'count    int unsigned not null default 0'
	pp.subst'pos      int unsigned'
	pp.subst'lang     char(2) character set ascii'
	pp.subst'currency char(3) character set ascii'
	pp.subst'country  char(2) character set ascii'
end

function M.package.mysql(pp)
	pp.require'mysql_quote'
	pp.require'mysql_ddl'
	pp.require'mysql_domains'
end

--self-test ------------------------------------------------------------------

if not ... then

	local spp = M.new()
	spp.require'mysql'
	for _,s in ipairs(spp.queries([[
	$id not null, $fk(store_currency, currency, currency),
]])) do
	print(s)
end

end


return M
