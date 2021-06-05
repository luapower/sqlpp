
--SQL parser and preprocessor with macros and named arguments.
--Written by Cosmin Apreutesei. Public Domain.

local fmt = string.format
local add = table.insert

local M = {package = {}, keywords = {}}

local function assertf(v, err, ...)
	if v then return v end
	err = fmt(err, ...)
	error(err, 2)
end

local esc = {
	['0']  = string.char(0),
	['b']  = '\b',
	['n']  = '\n',
	['r']  = '\r',
	['t']  = '\t',
	['Z']  = string.char(26),
	['\\'] = '\\',
}

function M.new()

	local pp = {package = {}}

	--sql parsing -------------------------------------------------------------

	pp.ansi_quotes = false --true: treat double-quoted strings as identifiers.

	local function tokens(s)
		local i = 1
		local yield = coroutine.yield
		::again::
		local i1, c, j = s:match('^%s*()(.)()', i)
		if not c then --eof
			return
		end
		if i1 > i then
			yield('space', i, i1)
			i = i1
			if i1 == #s then --eof
				return
			end
		end
		if c == '-' then
			local j = s:match('^%-%s+.-\r?\n?()', j) -- `-- ...` comment
			if j then
				yield('comment', i, j)
				i = j
				goto again
			end
		end
		if c == '#' then
			local j = s:match('^.-\r?\n()', j) or #s -- `# ...` comment
			yield('comment', i, j)
			i = j
			goto again
		end
		if c == '/' then
			local j = s:match('^%*.-%*/()', j) -- `/* ... */` comment
			yield('comment', i, j)
			i = j
			goto again
		end
		if c == '\'' or c == '"' or c == '`' then --string or backtick ident
			while true do
				local patt = '()(['..c..'\\])()(.)'
				local i1, c, j1, c2 = s:match(patt, j)
				if not c then
					yield('error', i, i, 'unfinished string')
					return
				end
				j = j1
				if c == '\\' then -- backslash escape
					if c2 == '' then
						yield('error', i1+1, i1+1, 'unfinished backslash escape')
						return
					end
					j = j+1
				elseif c2 == c then -- `foo''s bar` or `foo""s bar` quote-quote
					j = j + 1
				else --end of string
					break
				end
			end
			local token = (c == '`' or (c == '"' and pp.ansi_quotes))
				and 'ident' or 'string'
			yield(token, i, j)
			i = j
			goto again
		end
		if c == ';' then --end of statement
			yield(';', i, j)
			i = j
			goto again
		end
		local j = s:match('^0x[0-9a-fA-F]+()', i) --hex literal
		if j then
			yield('number', i, j)
			i = j
			goto again
		end
		local j = s:match('^0b[01]+()', i) --binary literal
		if j then
			yield('number', i, j)
			i = j
			goto again
		end
		local j = s:match('^[%-%+]?%d*%.?%d*[eE]?[%-%+]?%d*()', i) --number
		if j and j > i and s:find'%d' then
			yield('number', i, j)
			i = j
			goto again
		end
		local j = s:match('^[%a_$][%w_$]*()', i) --keyword or identifier

		if j then
			yield('ident', i, j)
			i = j
			goto again
		end
		local j = s:match('[^%s]+()', i) --anything else: operator
		if j then
			yield('op', i, j)
			i = j
			goto again
		end
		yield('error', i1, #s, 'invalid token')
	end

	function pp.tokens(s)
		local tokens = coroutine.wrap(tokens)
		return function()
			return tokens(s)
		end
	end

	--arg substitution --------------------------------------------------------

	local keywords = {} --{sym->k}
	pp.keyword = {}
	setmetatable(pp.keyword, {
		__index = function(t, k)
			local sym = {}
			t[k] = sym
			keywords[sym] = k
			return sym
		end,
	})

	local _= pp.keyword.null
	local _= pp.keyword.default

	function pp.string(s) --stub
		return s
	end

	function pp.number(x) --stub
		return fmt('%0.17g', v) --max precision, min length.
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
		elseif keywords[v] then
			return keywords[v]
		else
			return nil, 'invalid value ' .. v
		end
	end

	function pp.named_params(sql, t)
		local names = {}
		local sql = sql:gsub(':([%w_:]+)', function(k)
			add(names, k)
			local v, err = pp.value(t[k])
			return assertf(v, 'param %s: %s\n%s', k, err, sql)
		end)
		return sql, names
	end

	function pp.indexed_params(sql, t)
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

	function pp.params(sql, ...)
		local param_values = type((...)) ~= 'table' and {...} or ...
		local sql = pp.indexed_params(sql, param_values)
		return pp.named_params(sql, param_values)
	end

	--macro substitution ------------------------------------------------------

	local substs = {}

	function pp.subst(def) --'name type'
		local name, val = def:match'([%w_]+)%s+(.*)'
		assertf(not substs[name], 'macro already defined: %s', name)
		substs[name] = val
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

	function pp.preprocess(sql)
		sql = sql:gsub('%-%-[^\r\n]*', '') --remove comments
		sql = sql:gsub('$([%w_]+)(%b())', macro_subst)
		sql = sql:gsub('$([%w_]+)', substs)
		return sql
	end

	--module system -----------------------------------------------------------

	function pp.require(pkg)
		for pkg in pkg:gmatch'[^%s]+' do
			if not pp.package[pkg] then
				assertf(M.package[pkg], 'no macro module: %s', pkg)(pp)
				pp.package[pkg] = true
			end
		end
	end

	return pp
end

--mysql ----------------------------------------------------------------------

function M.package.mysql_quote(pp)

	function pp.string(v)
		return s --TODO
	end

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
		return query1([[
			select c.table_name from information_schema.table_constraints c
			where c.table_schema = ? and c.constraint_name = ?
		]], config'db_name', name)
	end

	pp.allow_drop = true

	function pp.macro.drop_fk(name)
		if not pp.allow_drop then return end
		local tbl = constable(name)
		if not tbl then return end
		return _('alter table ?? drop foreign key ??', tbl, name)
	end

	function pp.macro.drop_table(name)
		if not pp.allow_drop then return end
		return fmt('drop table if exists %s', name)
	end

	local function fkname(tbl, col)
		return fmt('fk_%s_%s', tbl, col:gsub('%s', ''):gsub(',', '_'))
	end

	function pp.macro.fk(tbl, col, ftbl, fcol, ondelete, onupdate)
		ondelete = ondelete or 'restrict'
		onupdate = onupdate or 'cascade'
		local a1 = ondelete ~= 'restrict' and ' on delete '..ondelete or ''
		local a2 = onupdate ~= 'restrict' and ' on update '..onupdate or ''
		return fmt('constraint %s foreign key (%s) references %s (%s)',
			fkname(tbl, col), col, ftbl, fcol or col, a1, a2)
	end

	function pp.macro.uk(tbl, col)
		return fmt('constraint uk_%s_%s unique key (%s)',
			tbl, col:gsub('%s', ''):gsub(',', '_'), col)
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
		query(pp.params('$create_database(??);', name))
	end

	function pp.drop_table(s)
		for name in s:gmatch'[^%s]+' do
			query(pp.params('$drop_table(??);', name))
		end
	end

end

function M.package.mysql_domains(pp)
	pp.subst'id      int unsigned'
	pp.subst'bigid   bigint unsigned'
	pp.subst'pk      int unsigned primary key auto_increment'
	pp.subst'bigpk   bigint unsigned primary key auto_increment'
	pp.subst'name    varchar(64)'
	pp.subst'email   varchar(128)'
	pp.subst'hash    varchar(64) character set ascii'
	pp.subst'url     varchar(2048)'
	pp.subst'bool    tinyint not null default 0'
	pp.subst'bool1   tinyint not null default 1'
	pp.subst'atime   timestamp not null'
	pp.subst'ctime   timestamp not null default current_timestamp'
	pp.subst'mtime   timestamp not null on update current_timestamp'
	pp.subst'money   decimal(20,6)'
	pp.subst'qty     decimal(20,6)'
	pp.subst'percent decimal(20,6)'
	pp.subst'count   int unsigned not null default 0'
	pp.subst'lang    char(2) character set ascii not null'
end

function M.package.mysql(pp)
	pp.require'mysql_quote'
	pp.require'mysql_ddl'
	pp.require'mysql_domains'
end

--self-test ------------------------------------------------------------------

if not ... then

	local pp = M.new()
	pp.require'mysql'
	print(pp.preprocess[[
$create_database(foo);
$drop_table(bar);
$table foo (
	foo_id    $pk, $fk(t, f, t2, f2),
	foo_name  $name
);
]])

	local sql = [[
		select * from `table` where name = 'so\'me foo''s bar' and id <= 5;
	]]
	for tk, i, j, err in pp.tokens(sql) do
		print(tk, i, j, i and j and sql:sub(i, j-1), err or '')
	end

end


return M
