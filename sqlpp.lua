
--SQL preprocessor, postprocessor and command API.
--Written by Cosmin Apreutesei. Public Domain.

local glue = require'glue'
local errors = require'errors'

local fmt = string.format
local add = table.insert
local concat = table.concat
local char = string.char

local assertf = glue.assert
local repl = glue.repl
local attr = glue.attr
local update = glue.update
local empty = glue.empty
local names = glue.names

local M = {package = {}}

function M.new()

	local spp = {}
	local cmd = {}
	spp.command = cmd

	--parsing string literals -------------------------------------------------

	function collect_strings(s, dt)
		local i = 1
		local t = {}
		::next_string::
		local i1 = s:find("'", i, true)
		if i1 then --string literal start
			local i2
			local j = i1 + 1
			::again::
			local j1 = s:find("\\'", j, true) --skip over \'
			if j1 then
				j = j1 + 2
				goto again
			end
			local j1 = s:find("''", j, true) --skip over ''
			if j1 then
				j = j1 + 2
				goto again
			end
			i2 = s:find("'", j, true) --string literal end
			if i2 then
				add(t, s:sub(i, i1 - 1))
				add(dt, s:sub(i1, i2))
				add(t, '\0'..char(#dt))
				i = i2 + 1
				goto next_string
			else
				error'string literal not closed'
			end
		else
			add(t, s:sub(i))
		end
		return concat(t)
	end

	--conditional compilation -------------------------------------------------

	--Process #if #elif #else #endif conditionals.
	--Also normalize newlines and remove single-line comments which the mysql
	--client protocol cannot parse. Multiline comments are not removed since
	--they can be used for optimizer hints.
	local globals_mt = {__index = _G}
	local function parse_expr(s, params)
		local f = assert(loadstring('return '..s))
		params = update({}, params) --copy it so we alter it
		setmetatable(params, globals_mt)
		setfenv(f, params)
		return f()
	end
	local function spp_ifs(sql, params)
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

	spp.keywords = {} --{sym->k}
	spp.keyword = {}
	setmetatable(spp.keyword, {
		__index = function(t, k)
			local sym = {}
			t[k] = sym
			spp.keywords[sym] = k
			return sym
		end,
	})

	local _ = spp.keyword.null
	local _ = spp.keyword.default

	function cmd:sqlstring(s)
		return "'"..self:quote(s).."'"
	end

	function cmd:sqlnumber(x) --stub
		return fmt('%0.17g', x) --max precision, min length.
	end

	function cmd:sqlboolean(v) --stub
		return tostring(v)
	end

	--NOTE: don't use dots in db names, table names and column names!
	local function add_ticks(s)
		return '`'..s..'`'
	end
	function cmd:sqlname(s)
		assert(s, 'sql name missing')
		if s:sub(1, 1) == '`' then
			return s
		end
		return s:gsub('[^%.]+', add_ticks)
	end

	function cmd:sqlval(v, field)
		local to_sql = field and field.to_sql
		if to_sql then
			return to_sql(v)
		elseif v == nil then
			return 'null'
		elseif type(v) == 'number' then
			return self:sqlnumber(v)
		elseif type(v) == 'string' then
			return self:sqlstring(v)
		elseif type(v) == 'boolean' then
			return self:sqlboolean(v)
		elseif type(v) == 'table' then
			if #v > 0 then --list: for use in `in (?)`
				local t = {}
				for i,v in ipairs(v) do
					t[i] = self:sqlval(v, field)
				end
				return concat(t, ', ')
			else --empty list: good for 'in (?)' but NOT GOOD for `not in (?)` !!!
				return 'null'
			end
		elseif spp.keywords[v] then
			return spp.keywords[v]
		else
			error('invalid value ' .. v)
		end
	end

	function cmd:binval(v, field)
		local to_bin = field and field.to_bin
		if to_bin then
			return to_bin(v)
		else
			return v
		end
	end

	--macros ------------------------------------------------------------------

	local defines = {}

	function spp.subst(def) --'name type'
		local name, val = def:match'([%w_]+)%s+(.*)'
		assertf(not defines[name], 'macro already defined: %s', name)
		defines[name] = val
	end

	spp.macro = {}

	local function macro_arg(self, arg, t)
		local k = arg:match'^:([%w_][%w_%:]*)'
		if k then --unparsed param expansion.
			return t[k]
		else --parsed param expansion.
			return self:sqlparams(arg, t)
		end
	end

	local function macro_subst(self, name, args, t)
		local macro = assertf(spp.macro[name], 'undefined macro: $%s()', name)
		args = args:sub(2,-2)..','
		local dt = {}
		for arg in args:gmatch'([^,]+)' do
			arg = glue.trim(arg)
			dt[#dt+1] = macro_arg(self, arg, t) --expand params in macro args *unquoted*!
		end
		return macro(unpack(dt))
	end

	--named params & positional args substitution -----------------------------

	function cmd:sqlparams(sql, vals)
		local names = {}
		return sql:gsub('::([%w_]+)', function(k) -- ::col, ::table, etc.
				add(names, k)
				local v, err = self:sqlname(vals[k])
				return assertf(v, 'param %s: %s\n%s', k, err, sql)
			end):gsub(':([%w_][%w_%:]*)', function(k) -- :foo, :foo:old, etc.
				add(names, k)
				local v, err = self:sqlval(vals[k])
				return assertf(v, 'param %s: %s\n%s', k, err, sql)
			end), names
	end

	function cmd:sqlargs(sql, vals)
		local i = 0
		return (sql:gsub('%?%?', function() -- ??
				i = i + 1
				local v, err = self:sqlname(vals[i])
				return assertf(v, 'param %d: %s\n%s', i, err, sql)
			end):gsub('%?', function() -- ?
				i = i + 1
				local v, err = self:sqlval(vals[i])
				return assertf(v, 'param %d: %s\n%s', i, err, sql)
			end))
	end

	--preprocessor ------------------------------------------------------------

	local function args_params(...)
		local args = select('#', ...) > 0 and {...} or empty
		local params = type((...)) == 'table' and (...) or empty
		return args, params
	end

	local function sqlquery(self, prepare, sql, ...)

		local args, params = args_params(...)

		if not sql:find'[#$:?{%-;]' then --nothing to see here
			return sql, empty
		end

		local sql = spp_ifs(sql, params) --#if ... #endif

		--We can't just expand values on-the-fly in multiple passes of gsub()
		--because each pass would result in a partially-expanded query with
		--string literals inside so the next pass would parse inside those
		--literals. To avoid that, we replace expansion points inside the query
		--with special markers and on a second step we replace the markers
		--with the expanded values.

		--step 1: find all expansion points and replace them with a marker
		--that string literals can't contain.

		local repl = {}

		--collect string literals
		sql = collect_strings(sql, repl)

		--collect macros
		local macros = {}
		sql = sql:gsub('$([%w_]+)(%b())', function(name, args)
				add(macros, name)
				add(macros, args)
				return '\0'..char(#repl + #macros / 2)
			end) --$foo(arg1,...)
		for i = 1, #macros, 2 do
			local m_name, m_args = macros[i], macros[i+1]
			add(repl, macro_subst(self, m_name, m_args, params) or '')
		end

		--collect defines
		sql = sql:gsub('$([%w_]+)', function(name)
				add(repl, assertf(defines[name], '$%s is undefined', name))
				return '\0'..char(#repl)
			end) --$foo

		local param_names = {}

		--collect verbatims
		sql = glue.subst(sql, function(name)
				add(param_names, name)
				add(repl, assertf(params[name], '{%s} is missing', name))
				return '\0'..char(#repl)
			end) --{foo}

		local param_map = prepare and {}

		--collect named params
		sql = sql:gsub('::([%w_]+)', function(k) -- ::col, ::table, etc.
				add(param_names, k)
				local v, err = self:sqlname(params[k])
				add(repl, assertf(v, 'param %s: %s', k, err))
				return '\0'..char(#repl)
			end):gsub(':([%w_][%w_%:]*)', function(k) -- :foo, :foo:old, etc.
				add(param_names, k)
				if prepare then
					add(param_map, k)
					add(repl, '?')
				else
					local v, err = opt and opt.prepare and '?' or self:sqlval(params[k])
					add(repl, assertf(v, 'param %s: %s', k, err))
				end
				return '\0'..char(#repl)
			end)

		--collect indexed params
		local i = 0
		sql = sql:gsub('%?%?', function() -- ??
				i = i + 1
				local v, err = self:sqlname(args[i])
				add(repl, assertf(v, 'param %d: %s', i, err))
				return '\0'..char(#repl)
			end):gsub('%?', function() -- ?
				i = i + 1
				if prepare then
					add(param_map, i)
					add(repl, '?')
				else
					local v, err = self:sqlval(args[i])
					add(repl, assertf(v, 'param %d: %s', i, err))
				end
				return '\0'..char(#repl)
			end)

		assert(not (#param_names > 0 and i > 0),
			'both named params and positional args found')

		--step 3: expand markers.

		sql = sql:gsub('%z(.)', function(ci)
			local i = string.byte(ci)
			return repl[i]
		end)

		return sql, param_names, param_map
	end

	function cmd:sqlquery(sql, ...)
		return sqlquery(self, nil, sql, ...)
	end

	function cmd:sqlprepare(sql, ...)
		return sqlquery(self, true, sql, ...)
	end

	local function map_params(stmt, cmd, param_map, ...)
		local args, params = args_params(...)
		local t = {}
		for i,k in ipairs(param_map) do
			local v
			if type(k) == 'number' then --arg
				v = args[k]
			else --param
				v = params[k]
			end
			t[i] = cmd:binval(v, stmt.params[i])
			t.n = i
		end
		return t
	end

	--TODO: this gives false positives (but no false negatives which is what we want).
	function cmd:has_ddl(sql)
		sql = glue.trim(sql):lower()
		return
		      sql:find'^create%s'
			or sql:find'^alter%s'
			or sql:find'^drop%s'
			or sql:find'^grant%s'
			or sql:find'^revoke%s'
			or sql:find';%s*create%s'
			or sql:find';%s*alter%s'
			or sql:find';%s*drop%s'
			or sql:find';%s*grant%s'
			or sql:find';%s*revoke%s'
			and true or false
	end

	--row list formatting -----------------------------------------------------

	function cmd:sqlrows(rows, opt) --{{v1,...},...} -> '(v1,...),\n (v2,...)'
		local max_sizes = {}
		local pad_dirs = {}
		local srows = {}
		local as_cols = {} --{as_col1,...}
		local as_col_map = {} --{as_col->col}
		if opt.col_map then
			for col, as_col in glue.sortedpairs(opt.col_map) do
				add(as_cols, as_col)
				as_col_map[as_col] = col
			end
		elseif opt.fields then
			for i, field in ipairs(opt.fields) do
				local as_col = opt.compact and i or col
				add(as_cols, as_col)
				as_col_map[as_col] = field.name
			end
		elseif opt.n then
			for i = 1, opt.n do
				as_cols[i] = i
			end
		end
		for ri,row in ipairs(rows) do
			local srow = {}
			srows[ri] = srow
			for i,as_col in ipairs(as_cols) do
				local v = row[as_col]
				if type(v) == 'function' then --self-generating value.
					v = v()
				end
				pad_dirs[i] = type(v) == 'number' and 'l' or 'r'
				local col = as_col_map[as_col]
				local field = opt.fields and opt.fields[col]
				local s = self:sqlval(v, field)
				srow[i] = s
				max_sizes[i] = math.max(max_sizes[i] or 0, #s)
			end
		end
		local dt = {}
		local prefix = (opt and opt.indent or '')..'('
		for ri,srow in ipairs(srows) do
			local t = {}
			for i,s in ipairs(srow) do
				t[i] = glue.pad(s, max_sizes[i], ' ', pad_dirs[i])
			end
			dt[ri] = prefix..concat(t, ', ')..')'
		end
		return concat(dt, ',\n')
	end

	--tab-separated rows parsing ----------------------------------------------

	function spp.tsv_rows(t, s) --{n=3|cols='3 1 2', transform1, ...}
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

	function cmd:sqltsv(t, s)
		return self:sqlrows(spp.tsv_rows(t, s), t.rows)
	end

	--row grouping ------------------------------------------------------------

	local function group_key(col)
		return (type(col) == 'string' or type(col) == 'number')
			and function(e) return e[col] end
			or col
	end

	function spp.groups(col, items, group_store_key)
		local t = {}
		local k, st
		local group_key = group_key(col)
		for i,e in ipairs(items) do
			local k1 = group_key(e)
			if not st or k1 ~= k then
				st = {}
				if group_store_key ~= nil then
					st[group_store_key] = k1
				end
				t[#t+1] = st
			end
			st[#st+1] = e
			k = k1
		end
		return t
	end

	function spp.each_group(col, items)
		local group_key = group_key(col)
		local groups = spp.groups(col, items)
		local i, n = 1, #groups
		return function()
			if i > n then return end
			local items = groups[i]
			i = i + 1
			return i-1, group_key(items[1]), items
		end
	end

	--module system -----------------------------------------------------------

	spp.package = {}

	function spp.require(pkg)
		for pkg in pkg:gmatch'[^%s]+' do
			if not spp.package[pkg] then
				assertf(M.package[pkg], 'no sqlpp module: %s', pkg)(spp)
				spp.package[pkg] = true
			end
		end
	end

	--command API -------------------------------------------------------------

	spp.errno = {} --{errno->f(err)}

	function cmd:assert(ret, ...)
		if ret ~= nil then
			return ret, ...
		end
		local msg, errno, sqlstate = ...
		local err = errors.new('db',
			{
				sqlcode = errno,
				sqlstate = sqlstate,
				addtraceback = true,
			},
			'%s%s%s', msg,
				errno and ' ['..errno..']' or '',
				sqlstate and ' '..sqlstate or ''
		)
		local parse = spp.errno[errno]
		if parse then
			parse(self, err)
		end
		errors.raise(err)
	end

	function spp.connect(opt)
		local self = update({}, cmd)
		self.rawconn = self:assert(self:rawconnect(opt))
		return self
	end

	function spp.use(rawconn)
		local self = update({}, cmd)
		self.rawconn = self:rawuse(rawconn)
		return self
	end

	function process_result_set(self, rows, fields, opt)
		if not fields then --not a select query.
			return
		end
		local fa = opt.field_attrs
		for i,f in ipairs(fields) do
			update(f, fa and fa[f.name])
			if f.origin_table and f.schema then
				local tdef = self:table_def(f.schema..'.'..f.origin_table)
				if tdef then
					update(f, tdef.fields[f.origin_name])
				end
			end
		end
		for i,f in ipairs(fields) do
			local convert = f.to_lua
			if convert then
				for i = 1, #rows do
					if #fields == 1 and opt.to_array then
						rows[i] = convert(rows[i])
					elseif opt.compact then
						local row = rows[i]
						local fi = fields[col].index
						row[fi] = convert(row[fi])
					else
						local row = rows[i]
						local fi = fields[col].name
						row[fi] = convert(row[fi])
					end
				end
			end
		end
	end

	local function get_result_sets(self, results, opt, param_names, ret, ...)
		if ret == nil then return nil, ... end --error
		local rows, again, fields = ret, ...
		results = results or (again and {param_names = param_names}) or nil
		if results then
			add(results, {rows, fields})
			if again then
				return get_result_sets(self, results, opt, param_names,
					self:assert(self:rawagain(opt)))
			else
				for _,res in ipairs(results) do
					process_result_set(self, res[1], res[2], opt)
				end
				return results
			end
		else
			process_result_set(self, rows, fields, opt)
			return rows, fields, param_names
		end
	end

	function cmd:query(opt, sql, ...)

		if type(opt) ~= 'table' then --sql, ...
			return self:query(empty, opt, sql, ...)
		elseif type(sql) == 'table' then --opt1, opt2, sql, ...
			return self:query(update(opt, sql), ...)
		end

		local param_names
		if opt.parse ~= false then
			sql, param_names = self:sqlquery(sql, ...)
		end

		if self:has_ddl(sql) then
			self:schema_changed()
		end
		return get_result_sets(self, nil, opt, param_names,
			self:assert(self:rawquery(sql, opt)))
	end

	cmd.exec_with_options = cmd.query

	local function pass(rows, ...)
		if rows and (...) then
			return rows[1], ...
		else
			return rows, ...
		end
	end
	function cmd:first_row(...)
		return pass(self:exec_with_options({to_array=1}, ...))
	end

	function cmd:each_group(col, ...)
		local rows = self:exec_with_options(nil, ...)
		return spp.each_group(col, rows)
	end

	function cmd:each_row(...)
		local rows = self:exec_with_options({to_array=1}, ...)
		return ipairs(rows)
	end

	function cmd:each_row_vals(...)
		local rows, cols = self:exec_with_options({compact=1, to_array=1}, ...)
		local i, n, cn = 1, #rows, #cols
		return function()
			if i > n then return end
			local row = rows[i]
			i = i + 1
			if cn == 1 then --because to_array=1, row is val in this case
				return i-1, row
			else
				return i-1, unpack(row, 1, cn)
			end
		end
	end

	function cmd:exec(...)
		return self:exec_with_options(nil, ...)
	end

	function cmd:prepare(opt, sql, ...)

		if type(opt) ~= 'table' then --sql, ...
			return self:prepare(empty, opt, sql, ...)
		elseif type(sql) == 'table' then --opt1, opt2, sql, ...
			return self:prepare(update(opt, sql), ...)
		end

		local param_names, param_map
		if opt.parse ~= false then
			sql, param_names, param_map = self:sqlprepare(sql, ...)
		end

		local cmd = self
		local function pass(rawstmt, ...)
			if rawstmt == nil then return nil, ... end

			local stmt = {
				exec          = cmd.exec,
				first_row     = cmd.first_row,
				each_group    = cmd.each_group,
				each_row      = cmd.each_row,
				each_row_vals = cmd.each_row_vals,
			}

			function stmt:free()
				return cmd:rawstmt_free(rawstmt)
			end

			function stmt:exec_with_options(exec_opt, ...)
				local t = map_params(self, cmd, param_map, ...)
				local opt = exec_opt and update(exec_opt, opt) or opt
				return get_result_sets(cmd, nil, opt, param_names,
					cmd:assert(cmd:rawstmt_query(rawstmt, opt, unpack(t, 1, t.n))))
			end

			return stmt, param_names
		end
		return pass(self:assert(self:rawprepare(sql, opt)))
	end

	function cmd:atomic(f, ...)
		self:query('start transaction')
		local function pass(ok, ...)
			self:query(ok and 'commit' or 'rollback')
			return assert(ok, ...)
		end
		return pass(glue.pcall(f, ...))
	end

	--cached table defs -------------------------------------------------------

	function cmd:schema_changed()
		self._schema_cache = nil
	end

	function cmd:schema_cache()
		return attr(self, '_schema_cache')
	end

	local function strip_ticks(s)
		return s:gsub('^`', ''):gsub('`$', '')
	end
	local function sch_tbl_arg(self, sch_tbl)
		local sch, tbl = sch_tbl:match'^(.-)%.(.*)$'
		if not sch then
			sch, tbl = assert(self.schema), sch_tbl
		end
		return strip_ticks(sch), strip_ticks(tbl)
	end

	spp.table_attrs = {} --{sch.tbl->attrs}
	spp.col_attrs = {} --{sch.tbl.col->attrs}
	spp.col_type_attrs = {} --{col_type->attrs}
	spp.col_name_attrs = {} --{col_name->attrs}

	function cmd:table_def(sch_tbl)
		local cache = self:schema_cache()
		local def = cache[sch_tbl]
		if not def then
			local sch, tbl = sch_tbl_arg(self, sch_tbl)
			def = self:get_table_def(sch, tbl)
			update(def, spp.table_attrs[sch..'.'..tbl])
			if def then
				cache[sch_tbl] = def
				for _,field in ipairs(def.fields) do
					local col = field.name
					local col_attrs = spp.col_attrs[sch..'.'..tbl..'.'..col]
					update(field, col_attrs) --allow col_attrs to change field's type.
					local col_type_attrs = spp.col_type_attrs[field.type]
					local col_name_attrs = spp.col_name_attrs[col]
					update(field, col_type_attrs, col_name_attrs)
					def.fields[col] = field
				end
			end
		end
		return def
	end

	--DDL macros --------------------------------------------------------------

	spp.subst'table  create table if not exists'

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

	function spp.macro.fk(tbl, col, ftbl, fcol, ondelete, onupdate)
		ondelete = ondelete or 'restrict'
		onupdate = onupdate or 'cascade'
		local a1 = ondelete ~= 'restrict' and ' on delete '..ondelete or ''
		local a2 = onupdate ~= 'restrict' and ' on update '..onupdate or ''
		return fmt('constraint %s foreign key (%s) references %s (%s)%s%s',
			fkname(tbl, col), cols(col), ftbl, cols(fcol or col), a1, a2)
	end

	function spp.macro.child_fk(tbl, col, ftbl, fcol)
		return spp.macro.fk(tbl, col, ftbl, fcol, 'cascade')
	end

	function spp.macro.uk(tbl, col)
		return fmt('constraint %s unique key (%s)', ukname(tbl, col), cols(col))
	end

	local function ixcols(s)
		return cols(s)
			:gsub(',asc$', ' asc'):gsub(',desc$', ' desc')
			:gsub(',asc,', ' asc,'):gsub(',desc,', ' desc,')
	end
	function spp.macro.ix(tbl, col)
		return fmt('index %s (%s)', ixname(tbl, col), ixcols(col))
	end

	--DDL commands ------------------------------------------------------------

	--databases

	function cmd:create_database(name, charset, collation)
		charset = charset or ''
		return self:query(outdent[[
			create database if not exists ?:name
				#if charset
				character set {charset}
				#endif
				#if collation
				collate {collation}
				#endif
			]], {
				name = name,
				charset = charset,
				collation = collation,
			})
	end

	--tables

	function cmd:drop_table(name)
		return self:query('drop table if exists ??', name)
	end

	function cmd:drop_tables(s)
		local dt = {}
		for name in s:gmatch'[^%s]+' do
			dt[#dt+1] = self:drop_table(name)
		end
		return dt
	end

	--columns

	function cmd:add_column(tbl, name, type_pos)
		if self:column_exists(tbl, name) then return end
		return self:query(fmt('alter table ?? add column %s %s', name, type_pos), tbl)
	end

	function cmd:rename_column(tbl, old_name, new_name)
		if not self:column_exists(tbl, old_name) then return end
		return self:query('alter table ?? rename column ?? to ??', tbl, old_name, new_name)
	end

	function cmd:drop_column(tbl, name)
		if not self:column_exists(tbl, old_name) then return end
		return self:query('alter table ?? remove column ??', tbl, name)
	end

	--forein keys, indices, unique keys

	function cmd:add_fk(tbl, col, ftbl, ...)
		if self:index_exists(fkname(tbl, col)) then return end
		return self:query('alter table ?? add ' ..
			spp.macro.fk(self:sqlname(tbl), col, self:sqlname(ftbl), ...), tbl)
	end

	function cmd:add_uk(tbl, col)
		if self:index_exists(ukname(tbl, col)) then return end
		return self:query('alter table ?? add ' .. spp.macro.uk(self:sqlname(tbl), col), tbl)
	end

	function cmd:add_ix(tbl, col)
		if self:index_exists(ixname(tbl, col)) then return end
		return self:query('alter table ?? add ' .. spp.macro.ix(self:sqlname(tbl), col), tbl)
	end

	local function drop_index(self, type, tbl, col)
		local name = indexname(type, tbl, col)
		if not self:index_exists(name) then return end
		local s =
			   type == 'fk' and 'foreign key'
			or type == 'uk' and 'unique key'
			or type == 'ix' and 'index'
			or assert(false)
		return self:query(fmt('alter table ?? drop %s %s', s, name), tbl)
	end
	function cmd:drop_fk(tbl, col) return drop_index(self, 'fk', tbl, col) end
	function cmd:drop_uk(tbl, col) return drop_index(self, 'uk', tbl, col) end
	function cmd:drop_ix(tbl, col) return drop_index(self, 'ix', tbl, col) end

	function cmd:readd_fk(tbl, col, ...)
		if self:drop_fk(tbl, col) then
			return self:add_fk(tbl, col, ...)
		end
	end

	function cmd:readd_uk(tbl, col)
		if self:drop_uk(tbl, col) then
			return self:add_uk(tbl, col)
		end
	end

	function cmd:readd_ix(tbl, col)
		if self:drop_ix(tbl, col) then
			return self:add_ix(tbl, col)
		end
	end

	--MDL commands ------------------------------------------------------------

	function where_sql(self, vals, col_map, pk, field_map, security_filter)
		local t = {}
		for i, col in ipairs(pk) do
			local val_name = col_map[col]
			local v = vals[val_name]
			assert(v ~= nil)
			local field = field_map[col]
			if i > 1 then add(t, ' and ') end
			add(t, self:sqlname(col)..' = '..self:sqlval(v, field))
		end
		local sql = concat(t)
		if security_filter then
			if type(security_filter) == 'table' then --
				--
			end
			sql = '('..sql..') and ('..security_filter..')'
		end
		return sql
	end

	local function set_sql(self, vals, col_map, field_map)
		local t = {}
		for col, val_name in sortedpairs(col_map) do
			local v = vals[val_name]
			if v ~= nil then
				local field = field_map[col]
				add(t, self:sqlname(col)..' = '..self:sqlval(v, field))
			end
		end
		return #t > 0 and concat(t, ',\n\t')
	end

	local function pass(ret, ...)
		if not ret then return nil, ... end
		return repl(ret.insert_id, 0, nil)
	end
	function cmd:insert_row(tbl, vals, col_map)
		local tdef = self:table_def(tbl)
		local set_sql = set_sql(vals, col_map, tdef.fields)
		local sql
		if not set_sql then --no fields, special syntax.
			sql = fmt('insert into %s values ()', self:sqlname(tbl))
		else
			sql = fmt(outdent[[
				insert into %s set
					%s
			]], self:sqlname(tbl), set_sql)
		end
		return pass(self:query({parse = false}, sql))
	end

	function cmd:update_row(tbl, vals, col_map, security_filter)
		local tdef = self:table_def(tbl)
		local set_sql = set_sql(vals, col_map, tdef.fields)
		if not set_sql then
			return
		end
		local where_sql = where_sql(vals, col_map, tdef.pk, tdef.fields, security_filter)
		local sql = fmt(outdent[[
			update %s set
				%s
			where %s
		]], self:sqlname(tbl), set_sql, where_sql)
		return self:query({parse = false}, sql)
	end

	function cmd:delete_row(tbl, vals, col_map, security_filter)
		local tdef = self:table_def(tbl)
		local where_sql = where_sql(vals, col_map, tdef.pk, tdef.fields, security_filter)
		local sql = fmt(outdent[[
			delete from %s where %s
		]], self:sqlname(tbl), where_sql)
		return self:query({parse = false}, sql)
	end

	--NOTE: The returned insert_id is that of the first inserted row.
	--You do the math for the other rows, they should be consecutive even
	--while other inserts are happening at the same time but I'm not sure.
	function cmd:insert_rows(tbl, rows, col_map, compact)
		if #rows == 0 then
			return
		end
		local tdef = self:table_def(tbl)
		local rows_sql = self:sqlrows(rows, {
			col_map = col_map,
			fields = tdef.fields,
			compact = compact,
		})
		local t = {}
		for i,s in ipairs(glue.keys(col_map, true)) do
			t[i] = self:sqlname(s)
		end
		local cols_sql = concat(t, ', ')
		local sql = fmt(outdent[[
			insert into %s
				(%s)
			values
				%s
		]], self:sqlname(tbl), cols_sql, rows_sql)
		return pass(self:query({parse = false}, sql))
	end

	function cmd:update_from_select(vals, select_fields, update_tables)
		for i,tbl in names(update_tables) do
			local tdef = self:table_def(tbl)
			--select_fields
		end
	end

	return spp
end

return M
