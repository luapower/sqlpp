
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
local subst = glue.subst

local int_ranges = {
	tinyint   = {-(2^ 7-1), 2^ 7, 0, 2^ 8-1},
	shortint  = {-(2^15-1), 2^15, 0, 2^16-1},
	mediumint = {-(2^23-1), 2^23, 0, 2^24-1},
	int       = {-(2^31-1), 2^31, 0, 2^32-1},
	bigint    = {-(2^51-1), 2^51, 0, 2^52-1},
}

function sqlpp.package.mysql(spp)

	--command API driver ------------------------------------------------------

	local cmd = spp.command

	local function pass(self, cn, ...)
		if not cn then return cn, ... end
		self.schema = cn.schema
		function self:esc(s)
			return cn:esc(s)
		end
		function self:rawquery(sql, opt)
			return cn:query(sql, opt)
		end
		function self:rawagain(opt)
			return cn:read_result(opt)
		end
		function self:rawprepare(sql, opt)
			return cn:prepare(sql, opt)
		end
		return cn
	end
	function cmd:rawconnect(opt)
		return pass(self, mysql.connect(opt))
	end
	function cmd:rawuse(cn)
		return pass(self, cn)
	end

	function cmd:rawstmt_query(rawstmt, opt, ...)
		return rawstmt:query(opt, ...)
	end

	function cmd:rawstmt_free(rawstmt)
		rawstmt:free()
	end

	--mysql-specific quoting --------------------------------------------------

	local sqlnumber = cmd.sqlnumber
	function cmd:sqlnumber(v)
		if v ~= v or v == 1/0 or v == -1/0 then
			return 'null' --avoid syntax error for what ends up as null anyway.
		end
		return sqlnumber(self, v)
	end

	function cmd:sqlboolean(v)
		return v and 1 or 0
	end

	spp.col_type_attrs.bool = {
		from_server = function(self, v)
			if v == nil then return nil end
			return v ~= 0
		end,
	}

	spp.col_type_attrs.timestamp = {
		to_sql = function(v)
			if type(v) == 'number' then --timestamp
				return format('from_unixtime(%0.17g)', v)
			end
			return v
		end,
	}

	--DDL commands ------------------------------------------------------------

	spp.default_charset = 'utf8mb4'
	spp.default_collation = 'utf8mb4_unicode_ci'

	--existence tests

	function cmd:schema_exists(name)
		return self:first_row([[
			select 1 from information_schema.schemata
			where schema_name = ?
		]], name or self.schema) ~= nil
	end

	function cmd:table_exists(name)
		return self:first_row([[
			select 1 from information_schema.tables
			where table_schema = database() and table_name = ?
		]], name)
	end

	function cmd:fk_exists(name)
		return self:first_row([[
			select 1 from information_schema.referential_constraints
			where constraint_schema = database() and constraint_name = ?
		]], name) ~= nil
	end

	function cmd:index_exists(name)
		return self:first_row([[
			select 1 from information_schema.statistics
			where table_schema = database() and index_name = ?
		]], name) ~= nil
	end

	function cmd:column_exists(tbl, name)
		return self:first_row([[
			select 1 from information_schema.columns
			where table_schema = database() and table_name = ? and column_name = ?
		]], tbl, name) ~= nil
	end

	--check constraints

	function cmd:check_exists(tbl, name)
		return self:first_row([[
			sekect 1 from information_schema.check_constraints
			where table_schema = database() and table_name = ? and constraint_name = ?
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
		return self:query(fmt(outdent[[
			create procedure ::name (%s) sql security invoker
			begin
			%s
			end
		]], args or '', outdent(code)), {name = name})
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
			code[#code+1] = fmt(outdent([[
				if new.%s <=> old.%s then
					signal sqlstate '45000' set message_text = 'Read/only column: %s';
				end if;]], '\t'), col, col, col)
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

		for i,row in ipairs(self:rawquery(fmt([[
			select
				column_name,
				data_type,
				column_type,
				column_key,
				column_default,
				is_nullable,
				extra,
				character_maximum_length,
				numeric_precision,
				numeric_scale,
				character_set_name,
				collation_name
			from
				information_schema.columns
			where
				table_schema = %s and table_name = %s
			]], self:sqlval(sch), self:sqlval(tbl))))
		do

			local col = row.column_name
			local type = row.data_type
			local auto_increment = row.extra == 'auto_increment' or nil
			local unsigned = row.column_type:find' unsigned$' and true or nil

			if row.column_type == 'tinyint(1)' then --bool by convention
				type = 'bool'
				row.numeric_precision = nil
				row.numeric_scale = nil
			end

			if auto_increment then
				assert(not ai_col)
				ai_col = col
			end

			if row.column_key == 'PRI' then
				pk[#pk+1] = col
			end

			local digits = row.numeric_precision
			local decimals = row.numeric_scale
			local min, max = mysql.num_range(type, unsigned, digits, decimals)

			local field = {
				col = col,
				type = type,
				enum_values = parse_enum(row.column_type),
				auto_increment = auto_increment,
				not_null = row.is_nullable == 'NO' or nil,
				min = min,
				max = max,
				decimals = decimals,
				unsigned = unsigned,
				maxlen = row.character_maximum_length,
				charset = row.character_set_name,
				collation = row.collation_name,
				pri_key    = row.column_key == 'PRI' or nil,
				unique_key = row.column_key == 'UNI' or nil,
				indexed    = row.column_key == 'MUL' or nil,
			}
			fields[i] = field
			fields[col] = field

			field.default = self.rawconn.to_lua(row.column_default, field)

			--TODO: find a more general way to clear off non-constant defaults.
			if field.default then
				if type == 'datetime' or type == 'date' or type == 'timestamp' then
					if field.default:find'^CURRENT_TIMESTAMP' then
						field.default = nil
					end
				end
			end

		end

		for i, name, cols in spp.each_group('name', self:rawquery(fmt([[
			select
				constraint_name name,
				column_name col,
				referenced_table_schema ref_sch,
				referenced_table_name ref_tbl,
				referenced_column_name ref_col
			from
				information_schema.key_column_usage
			where
				table_schema = %s and table_name = %s
				and referenced_column_name is not null
			]], self:sqlval(sch), self:sqlval(tbl))))
		do
			if #cols == 1 then
				local row = cols[1]
				local field = fields[row.col:lower()]
				field.ref_table = (row.ref_sch..'.'..row.ref_tbl):lower()
				field.ref_col = row.ref_col:lower()
			else
				--TODO: comp-key fks
			end
		end

		return {schema = sch, name = tbl, fields = fields, pk = pk, ai_col = ai_col}
	end

	--error message parsing ---------------------------------------------------

	spp.errno[1364] = function(self, err)
		err.col = err.message:match"'(.-)'"
		err.message = _(S('error_field_required', 'Field "%s" is required'), err.col)
		err.code = 'required'
	end

	spp.errno[1048] = function(self, err)
		err.col = err.message:match"'(.-)'"
		err.message = _(S('error_field_not_null', 'Field "%s" cannot be empty'), err.col)
		err.code = 'not_null'
	end

	spp.errno[1062] = function(self, err)
		local pri = err.message:find"for key '.-%.PRIMARY'"
		err.code = pri and 'pk' or 'uk'
	end

	function spp.fk_message_remove()
		return 'Cannot remove {foreign_entity}: remove any associated {entity} first.'
	end

	function spp.fk_message_set()
		return 'Cannot set {entity}: {foreign_entity} not found in database.'
	end

	local function fk_message(self, err, op)
		local def = self:table_def(err.table)
		local fdef = self:table_def(err.fk_table)
		local t = {}
		t.entity = (def.text or def.name):lower()
		t.foreign_entity = (fdef.text or fdef.name):lower()
		local s = (op == 'remove' and spp.fk_message_remove or spp.fk_message_set)()
		return subst(s, t)
	end

	local function dename(s)
		return s:gsub('`', '')
	end
	local function errno_fk(self, err, op)
		local tbl, col, fk_tbl, fk_col =
			err.message:match"%((.-), CONSTRAINT .- FOREIGN KEY %((.-)%) REFERENCES (.-) %((.-)%)"
		if tbl:find'%.`#sql-' then --internal constraint from `alter table add foreign key` errors.
			return err
		end
		err.table = dename(tbl)
		err.col = dename(col)
		err.fk_table = dename(fk_tbl)
		err.fk_col = dename(fk_col)
		err.message = fk_message(self, err, op)
		err.code = 'fk'
	end
	spp.errno[1451] = function(self, err) return errno_fk(self, err, 'remove') end
	spp.errno[1452] = function(self, err) return errno_fk(self, err, 'set') end

end

function sqlpp.package.mysql_domains(spp)
	spp.subst'id       int unsigned'
	spp.subst'pk       int unsigned primary key auto_increment'
	spp.subst'bigid    bigint unsigned'
	spp.subst'bigpk    bigint unsigned primary key auto_increment'
	spp.subst'name     varchar(64)'
	spp.subst'strid    varchar(64) character set ascii'
	spp.subst'strpk    varchar(64) character set ascii primary key'
	spp.subst'email    varchar(128)'
	spp.subst'hash     varchar(64) character set ascii collate ascii_bin' --enough for tohex(hmac.sha256())
	spp.subst'url      varchar(2048) character set ascii'
	spp.subst'b64key   varchar(8192) character set ascii collate ascii_bin'
	spp.subst'bool     tinyint(1) not null default 0'
	spp.subst'bool1    tinyint(1) not null default 1'
	spp.subst'atime    timestamp not null default current_timestamp'
	spp.subst'ctime    timestamp not null default current_timestamp'
	spp.subst'mtime    timestamp not null default current_timestamp on update current_timestamp'
	spp.subst'money    decimal(15,3)' -- 999 999 999 999 . 999      (fits in a double)
	spp.subst'qty      decimal(15,6)' --     999 999 999 . 999 999  (fits in a double)
	spp.subst'percent  decimal(8,2)'  --         999 999 . 99
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
			password = 'root',
			schema = 'sp',
			collation = 'server',
		}

		spp.table_attrs['sp.val'] = {text = 'attribute value'}
		spp.table_attrs['sp.attr'] = {text = 'attribute'}
		spp.table_attrs['sp.combi_val'] = {text = 'attribute value combination'}

		if false then
			pp(cmd:table_def'usr')
		end

		if false then
			pp(cmd:query'select * from val limit 1; select * from attr limit 1')
		end

		if false then
			local stmt = assert(cmd:prepare('select * from val where val = :val'))
			pp(stmt:exec{val = 2})
		end

		if false then
			pp(cmd:query'insert into val (val, attr) values (100000000, 10000000)')
		end

		if true then
			pp(cmd:query'delete from val where val = 1')
		end

		if false then
			cmd:insert_rows('val',
				{{note1 = 'x', val1 = 'y'}},
				{note = 'note1', val = 'val1'}
			)
		end

	end)

end
