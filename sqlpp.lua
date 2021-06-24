
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
}

function M.new()

	local pp = {package = {}}

	--conditional compilation -------------------------------------------------

	--process #if #elif #else #endif conditionals.
	--also normalize newlines and remove single-line comments.
	local function parse_expr(s, params)
		local f = assert(loadstring('return '..s))
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
				add(t, line)
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
		return s:gsub('[%z\b\n\r\t\26\\]', esc)
	end

	function pp.number(x) --stub
		return fmt('%0.17g', x) --max precision, min length.
	end

	function pp.boolean(v) --stub
		return tostring(v)
	end

	function pp.name(v)
		if v:sub(1, 1) == '`' and v:sub(-1, -1) == '`' then
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
					t[i] = sqlval(v)
				end
				return table.concat(t, v.op or ', ')
			else --empty list: good for 'in (?)' but NOT GOOD for `not in (?)` !!!
				return 'null'
			end
		elseif pp.keywords[v] then
			return pp.keywords[v]
		else
			return nil, 'invalid value ' .. v
		end
	end

	local function named_params(sql, t)
		local names = {}
		local sql = sql:gsub(':([%w_:]+)', function(k)
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

	local function macro_subst(name, args)
		local macro = assertf(pp.macro[name], 'invalid macro: %s', name)
		args = args:sub(2,-2)..','
		local t = {}
		for arg in args:gmatch'([^,]+)' do
			arg = glue.trim(arg)
			t[#t+1] = arg
		end
		return macro(unpack(t))
	end

	local function pp_macros(sql)
		sql = sql:gsub('$([%w_]+)(%b())', macro_subst) --$foo(arg1,...)
		sql = sql:gsub('$([%w_]+)', defines) --$foo
		return sql
	end

	--preprocessor ------------------------------------------------------------

	function pp.query(sql, t)
		t = t or glue.empty
		local sql = glue.subst(sql, t) --{foo}
		local sql = pp_ifs(sql, t)  --#if ... #endif
		local sql = pp_macros(sql) --$foo and $foo(args...)
		local sql, names = pp.params(sql, t) -- ? ?? :foo :foo
		return sql, names
	end

	function pp.queries(sql, t)
		sql = pp.query(sql, t)
		local dt = {}
		for sql in sql:gmatch'[^;]+' do
			add(dt, sql)
		end
		return dt
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

	function pp.boolean(b)
		return v and 1 or 0
	end

end

function M.package.mysql_ddl(pp)

	pp.subst'table  create table if not exists'

	local function constable(name)
		local rows = pp.run_query([[
			select c.table_name from information_schema.table_constraints c
			where c.table_schema = ? and c.constraint_name = ?
		]], pp.db_name)
		return rows[1] --single-column result returned as array of values.
	end

	pp.allow_drop = false

	function pp.macro.drop_fk(name)
		if not pp.allow_drop then return end
		local tbl = constable(name)
		if not tbl then return end
		return fmt('alter table %s drop foreign key %s', tbl, name)
	end

	function pp.macro.drop_table(name)
		if not pp.allow_drop then return end
		return fmt('drop table if exists %s', name)
	end

	local function cols(s, sep)
		return s:gsub('%s+', sep or ',')
	end

	local function fkname(tbl, col)
		return fmt('fk_%s_%s', tbl, cols(col, '_'))
	end

	function pp.macro.fk(tbl, col, ftbl, fcol, ondelete, onupdate)
		ondelete = ondelete or 'restrict'
		onupdate = onupdate or 'cascade'
		local a1 = ondelete ~= 'restrict' and ' on delete '..ondelete or ''
		local a2 = onupdate ~= 'restrict' and ' on update '..onupdate or ''
		return fmt('constraint %s foreign key (%s) references %s (%s)%s%s',
			fkname(tbl, col), cols(col), ftbl, cols(fcol or col), a1, a2)
	end

	function pp.macro.uk(tbl, col)
		return fmt('constraint uk_%s_%s unique key (%s)',
			tbl, cols(col, '_'), cols(col))
	end

	function pp.macro.ix(tbl, col)
		return fmt('index ix_%s_%s (%s)', tbl, cols(col, '_'), cols(col))
	end

	function pp.macro.add_fk(tbl, col, ...)
		if constable(fkname(tbl, col)) then return end
		return fmt('alter table %s add %s', tbl, pp.macro.fk(tbl, col, ...))
	end

	function pp.macro.create_database(name)
		return fmt([[
create database if not exists %s
	character set utf8mb4
	collate utf8mb4_unicode_ci]], name)
	end

	function pp.create_database(name)
		pp.run_query(pp.query('$create_database(??);', name))
	end

	function pp.drop_table(s)
		for name in s:gmatch'[^%s]+' do
			pp.run_query(pp.query('$drop_table(??);', name))
		end
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
	pp.subst'atime    timestamp not null'
	pp.subst'ctime    timestamp not null default current_timestamp'
	pp.subst'mtime    timestamp not null default current_timestamp on update current_timestamp'
	pp.subst'money    decimal(20,6)'
	pp.subst'qty      decimal(20,6)'
	pp.subst'percent  decimal(20,6)'
	pp.subst'count    int unsigned not null default 0'
	pp.subst'pos      int unsigned'
	pp.subst'lang     char(2) character set ascii not null'
	pp.subst'currency char(3) character set ascii not null'
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
#if true
$create_database(foo);
#if true
$drop_table(bar);
#endif
$table foo (
	foo_id    $pk, $fk(t, f, t2, f2),
	foo_name  $name
);
#else
	bla bla bla
#endif
]])) do
	print(s)
end

end


return M
