
--MySQL preprocessor, postprocessor and command API.
--Written by Cosmin Apreutesei. Public Domain.

local sqlpp = require'sqlpp'
local glue = require'glue'
local mysql = require'mysql_client'

local fmt = string.format
local add = table.insert
local concat = table.concat

local repl = glue.repl
local outdent = glue.outdent
local sortedpairs = glue.sortedpairs

function sqlpp.package.mysql(spp)

	--command API driver ------------------------------------------------------

	local cmd = spp.command

	function cmd:rawconnect(opt)
		local cn = assert(mysql:new())
		self:assert(cn:connect(opt))
		self.dbname = opt and opt.database
		self._conn = cn
	end

	function cmd:rawquery(sql, opt)
		return self:assert(self._conn:query(sql, opt))
	end

	function cmd:rawagain(opt)
		return self:assert(self._conn:read_result(opt))
	end

	--mysql-specific quoting --------------------------------------------------

	spp.quote = mysql.quote

	local quote_number = spp.number
	function spp.number(v)
		if v ~= v or v == 1/0 or v == -1/0 then
			return 'null' --avoid syntax error for what ends up as null anyway.
		end
		return quote_number(v)
	end

	function spp.boolean(v)
		return v and 1 or 0
	end

	--DDL commands ------------------------------------------------------------

	--database defaults charset & collation

	spp.default_charset = 'utf8mb4'
	spp.default_collation = 'utf8mb4_unicode_ci'

	--existence tests for indices and columns

	function cmd:index_exists(name)
		return self:first_row([[
			select 1 from information_schema.innodb_indexes i
			where i.name = ?
		]], name) ~= nil --single-column result returned as array of values.
	end

	function cmd:column_exists(tbl, name)
		return self:first_row([[
			select 1 from information_schema.columns
			where table_schema = database() and table_name = ? and column_name = ?
		]], tbl, name) ~= nil
	end

	--triggers

	function cmd:trigger_exists(name)
		return self:first_row([[
			select 1 from information_schema.triggers
			where trigger_name = ?
		]], name) ~= nil
	end

	local add_trigger_sql = outdent[[
		create trigger ::name {where} on ::table for each row
		begin
		{code}
		end]]

	local function triggername(name, tbl, where)
		local s = where:gsub('([^%s])[^%s]*%s*', '%1')
		return fmt('%s_%s_%s', tbl, s, name)
	end

	function cmd:readd_trigger(name, tbl, where, code)
		local name = triggername(name, tbl, where)
		self:query('lock tables ?? write', tbl)
		self:query('drop trigger if exists ??', name)
		code = outdent(code, '\t')
		self:query(add_trigger_sql, {name = name, table = tbl, where = where, code = code})
		self:query('unlock tables')
	end

	function cmd:add_trigger(name, tbl, where, code)
		local name = triggername(name, tbl, where)
		if self:trigger_exists( name) then return end
		code = outdent(code, '\t')
		return self:query(add_trigger_sql, {name = name, table = tbl, where = where, code = code})
	end

	function cmd:drop_trigger(name, tbl, where)
		local name = triggername(name, tbl, where)
		return self:query('drop trigger if exists ??', name)
	end

	--procs

	function cmd:proc_exists(name)
		return self:first_row([[
			select 1 from information_schema.routines
			where routine_schema = database() and routine_name = ?
		]], name) ~= nil
	end

	function cmd:add_proc(name, args, code)
		if self:proc_exists(name) then return end
		code = outdent(code, '\t')
		return self:query(outdent[[
			create procedure ::name ({args})
			begin
			{code}
			end]], {name = name, args = args or '', code = code})
	end

	function cmd:drop_proc(name)
		return self:query('drop procedure if exists ??', name)
	end

	function cmd:readd_proc(name, ...)
		if self:drop_proc(name) then
			self:add_proc(name, ...)
		end
	end

	--column locks feature ----------------------------------------------------

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

	function cmd:add_column_locks(tbl, cols)
		return self:add_trigger('col_locks', tbl, 'before update', column_locks_code(cols))
	end

	function cmd:readd_column_locks(tbl, cols)
		return self:readd_trigger('col_locks', tbl, 'before update', column_locks_code(cols))
	end

	function cmd:drop_column_locks(tbl)
		return self:drop_trigger('col_locks', tbl, 'before update')
	end

	--table definitions -------------------------------------------------------

	local function parse_enum(s)
		local vals = s:match'^enum%((.-)%)$'
		if not vals then return end
		local t = {}
		vals:gsub("'(.-)'", function(s)
			t[#t+1] = s
		end)
		return t
	end

	function cmd:get_table_def(sch, tbl)

		local fields, pk, ai_col = {}, {}

		for i,row in self:each_row([[
			select
				c.column_name as col,
				c.data_type as col_type,
				c.column_type as full_type,
				c.column_key as col_key,
				c.column_default,
				c.is_nullable,
				c.extra,
				c.character_maximum_length as char_len,
				c.character_octet_length as byte_len,
				c.numeric_precision as col_precision,
				c.numeric_scale as col_scale,
				c.character_set_name as charset_name
			from
				information_schema.columns c
			where
				c.table_schema = ?
				and c.table_name = ?
			]], sch, tbl)
		do
			local col = row.col
			local is_ai = row.extra == 'auto_increment' or nil

			if is_ai then
				assert(not ai_col)
				ai_col = col
			end

			if row.col_key == 'PRI' then
				pk[#pk+1] = col
			end

			fields[i] = {
				name = col,
				type = row.col_type,
				enum_values = parse_enum(row.full_type),
				has_default = row.col_default ~= nil or nil,
				default = row.col_default,
				auto_increment = is_ai,
				not_null = row.is_nullable == 'NO' or nil,
				precision = row.col_precision,
				scale = row.col_scale,
				unsigned = row.full_type:find' unsigned$' and true or nil,
				char_len = row.char_len,
				byte_len = row.byte_len,
				charset = row.charset,
			}
		end

		return {fields = fields, pk = pk, ai_col = ai_col}
	end

end

function sqlpp.package.mysql_domains(spp)
	spp.subst'id       int unsigned'
	spp.subst'pk       int unsigned primary key auto_increment'
	spp.subst'bigid    bigint unsigned'
	spp.subst'bigpk    bigint unsigned primary key auto_increment'
	spp.subst'name     varchar(64)'
	spp.subst'email    varchar(128)'
	spp.subst'hash     varchar(64) character set ascii' --enough for tohex(hmac.sha256())
	spp.subst'url      varchar(2048) character set ascii'
	spp.subst'bool     tinyint not null default 0'
	spp.subst'bool1    tinyint not null default 1'
	spp.subst'atime    timestamp not null default current_timestamp'
	spp.subst'ctime    timestamp not null default current_timestamp'
	spp.subst'mtime    timestamp not null default current_timestamp on update current_timestamp'
	spp.subst'money    decimal(20,6)'
	spp.subst'qty      decimal(20,6)'
	spp.subst'percent  decimal(20,6)'
	spp.subst'count    int unsigned not null default 0'
	spp.subst'pos      int unsigned'
	spp.subst'lang     char(2) character set ascii'
	spp.subst'currency char(3) character set ascii'
	spp.subst'country  char(2) character set ascii'
end

if not ... then

	local spp = sqlpp.new()
	spp.require'mysql'

	if false then
		pp(spp.query(outdent[[
			select
				{verbatim}, '?', :foo, ::bar,
			from
			#if false
				no see
			#else
				see
			#endif
		]], {
			verbatim = 'can be anything',
			foo = 'FOO',
			bar = 'BAR.BAZ',
		}, 'xxx'))
	end

	local sock = require'sock'
	sock.run(function()

		local cmd = spp.connect{
			host = '127.0.0.1',
			port = 3307,
			user = 'root',
			password = 'abcd12',
			database = 'sp',
		}

		if false then
			pp(cmd:table_def'usr')
		end

		if true then
			pp(cmd:query'select * from val limit 1; select * from attr limit 1')
		end

		if false then
			cmd:insert_rows('val',
				{{note1 = 'x', val1 = 'y'}},
				{note = 'note1', val = 'val1'}
			)
		end

	end)

end