
--MySQL preprocessor, postprocessor and command API.
--Written by Cosmin Apreutesei. Public Domain.

if not ... then require'schema_test'; return end

local sqlpp = require'sqlpp'
local glue = require'glue'
local mysql = require'mysql_client'

local fmt = string.format
local add = table.insert
local cat = table.concat

local repl = glue.repl
local outdent = glue.outdent
local sortedpairs = glue.sortedpairs
local subst = glue.subst
local catargs = glue.catargs
local attr = glue.attr
local imap = glue.imap
local index = glue.index
local update = glue.update

function sqlpp.package.mysql(spp)

	--command API -------------------------------------------------------------

	local cmd = spp.command

	local function pass(self, cn, ...)
		if not cn then return cn, ... end
		self.engine = 'mysql'
		self.db = cn.db
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
		if opt and opt.fake then
			return {fake = true, host = '', port = '', esc = mysql.esc_utf8, engine = 'mysql'}
		end
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

	--quoting -----------------------------------------------------------------

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

	function cmd:get_reserved_words()
		if not self.rawquery then
			return {}
		end
		return index(self:assert(self:rawquery([[
			select lower(word) from information_schema.keywords where reserved = 1
		]], {compact = true})))
	end

	--DDL SQL -----------------------------------------------------------------

	function cmd:sqltype(fld)
		local mt = fld.mysql_type
		if mt == 'decimal' then
			return _('decimal(%d,%d)', fld.digits, fld.decimals)
		elseif mt == 'enum' or mt == 'set' then
			local function sqlval(s) return self:sqlval(s) end
			local vals = fld.enum_values or fld.set_values
			return _('%s(%s)', mt, cat(imap(vals, sqlval), ', '))
		elseif mt == 'varchar' then
			local maxlen = fld.maxlen or mysql.char_size(fld.size, fld.mysql_collation)
			return _('%s(%d)', mt, maxlen)
		elseif mt == 'varbinary' then
			return _('%s(%d)', mt, fld.size)
		else
			return mt
		end
	end

	--schema extraction -------------------------------------------------------

	local field_attrs = {
		digits=1,
		decimals=1,
		size=1, --not relevant for numbers, mysql_type is enough.
		maxlen=1,
		unsigned=1,
		not_null=1,
		auto_increment=1,
		comment=1,
		mysql_type=1,
		mysql_charset=1,
		mysql_collation=1,
		mysql_default=1,
	}

	local num_field_attrs = update({}, field_attrs)
	num_field_attrs.size = nil

	spp.schema_options = {
		supports_fks = true,
		supports_checks = true,
		supports_triggers = true,
		supports_procs = true,
		relevant_field_attrs = function(fld1, fld2)
			return fld2.type == 'number' and num_field_attrs or field_attrs
		end,
	}

	local function parse_values(s)
		local vals = s:match'%((.-)%)$'
		if not vals then return end
		local t = {}
		vals:gsub("'(.-)'", function(s)
			t[#t+1] = s
		end)
		return t
	end

	--input: data_type, column_type, numeric_precision, numeric_scale,
	--  ordinal_position, character_octet_length, character_set_name,
	--  collation_name, character_maximum_length.
	local function field_type_attrs(t)
		local mt = t.data_type
		local dt = {mysql_type = mt}
		dt.unsigned = t.column_type:find' unsigned$' and true or nil
		if mt == 'decimal' then
			dt.digits = t.numeric_precision
			dt.decimals = t.numeric_scale
			dt.type = dt.digits > 15 and 'decimal' or 'number'
			if dt.type == 'number' then
				min, max = mysql.dec_range(dt.digits, dt.decimals, dt.unsigned)
			end
		elseif mt == 'tinyint' or mt == 'smallint' or mt == 'mediumint'
			or mt == 'int' or mt == 'bigint'
		then
			dt.type = 'number'
			dt.min, dt.max, dt.size = mysql.int_range(mt, dt.unsigned)
			dt.decimals = 0
		elseif mt == 'float' then
			dt.type = 'number'
			dt.size = 4
		elseif mt == 'double' then
			dt.type = 'number'
			dt.size = 8
		elseif mt == 'year' then
			dt.type = 'number'
			dt.min, dt.max, dt.size = 1901, 2055, 2
		elseif mt == 'date' or mt == 'datetime' or mt == 'timestamp' then
			dt.type = 'date'
			dt.has_time = type ~= 'date' or nil
		elseif mt == 'enum' then
			dt.type = 'enum'
			dt.enum_values = parse_values(t.column_type)
		elseif mt == 'set' then
			dt.type = 'set'
			dt.set_values = parse_values(t.column_type)
		elseif mt == 'varchar' or mt == 'char'
			or mt == 'tinytext' or mt == 'text'
			or mt == 'mediumtext' or mt == 'longtext'
		then
			dt.type = 'text'
			dt.padded = mt == 'char' or nil
			dt.size = t.character_octet_length
			dt.maxlen = t.character_maximum_length
			dt.mysql_charset = t.character_set_name
			dt.mysql_collation = t.collation_name
		elseif mt == 'varbinary' or mt == 'binary'
			or mt == 'tinyblob' or mt == 'blob'
			or mt == 'mediumblob' or mt == 'longblob'
		then
			dt.type = 'blob'
			dt.size = t.character_octet_length
			dt.padded = mt == 'binary' or nil
		end
		return dt
	end

	function cmd:get_table_defs(db, tbl, opt)

		opt = opt or empty
		local tables = {} --{DB.TBL->table}

		for i, db_tbl, grp in spp.each_group('db_tbl', self:assert(self:rawquery(fmt([[
			select
				concat(table_schema, '.', table_name) db_tbl,
				table_name,
				column_name,
				ordinal_position,
				data_type,
				column_type,
				column_key,
				column_default,
				is_nullable,
				extra,
				character_maximum_length,
				character_octet_length,
				numeric_precision,
				numeric_scale,
				character_set_name,
				collation_name
			from
				information_schema.columns
			where
				]]..(catargs(' and ', '1 = 1',
						db  and 'table_schema = %s',
						tbl and 'table_name = %s') or '')..[[
			order by
				table_schema, table_name, ordinal_position
			]], db and self:sqlval(db), tbl and self:sqlval(tbl)))))
		do

			local fields, pk = {}, {}

			for i, row in ipairs(grp) do

				local col = row.column_name
				local auto_increment = row.extra == 'auto_increment' or nil

				local field = field_type_attrs(row)
				field.col = col
				field.col_index = row.ordinal_position
				field.auto_increment = auto_increment
				field.not_null = row.is_nullable == 'NO' or nil
				fields[i] = field
				fields[col] = field

				field.mysql_default = row.column_default
				field.default = row.column_default
				if field.type == 'date' and field.mysql_default == 'CURRENT_TIMESTAMP' then
					field.mysql_default = 'current_timestamp'
					field.default = nil
				end
			end

			tables[db_tbl] = {
				db = db, name = grp[1].table_name, fields = fields,
				pk = pk,
			}

		end

		local function row_col(row) return row.col end
		local function row_ref_col(row) return row.ref_col end

		local sql_db  = db  and self:sqlval(db)
		local sql_tbl = tbl and self:sqlval(tbl)

		for i, db_tbl, constraints in spp.each_group('db_tbl', self:assert(self:rawquery([[
			select
				concat(cs.table_schema, '.', cs.table_name) db_tbl,
				cs.table_name,
				kcu.column_name col,
				cs.constraint_name,
				cs.constraint_type,
				kcu.referenced_table_schema ref_db,
				kcu.referenced_table_name ref_tbl,
				kcu.referenced_column_name ref_col,
				coalesce(rc.update_rule, 'no action') as onupdate,
				coalesce(rc.delete_rule, 'no action') as ondelete
			from
				information_schema.table_constraints cs /* cs type: pk, fk, uk */
				left join information_schema.key_column_usage kcu /* fk ref_tbl & ref_cols */
					 on kcu.table_schema     = cs.table_schema
					and kcu.table_name       = cs.table_name
					and kcu.constraint_name  = cs.constraint_name
				left join information_schema.referential_constraints rc /* fk rules: innodb only */
					 on rc.constraint_schema = kcu.table_schema
					and rc.table_name        = kcu.table_name
					and rc.constraint_name   = kcu.constraint_name
			where
				cs.table_schema not in ('mysql', 'information_schema', 'performance_schema', 'sys')
				and ]]..(catargs(' and ',
						db  and 'cs.table_schema = '..sql_db,
						tbl and 'cs.table_name   = '..sql_tbl) or '1 = 1')..[[
			order by
				cs.table_schema, cs.table_name
			]])))
		do
			local tbl = tables[db_tbl]
			for i, cs_name, grp in spp.each_group('constraint_name', constraints) do
				local cs_type = grp[1].constraint_type
				if cs_type == 'PRIMARY KEY' then
					tbl.pk = imap(grp, row_col)
				elseif cs_type == 'FOREIGN KEY' then
					local ref_db  = grp[1].ref_db
					local ref_tbl = (ref_db ~= db and ref_db..'.' or '')..grp[1].ref_tbl
					if #grp == 1 then
						local field = tbl.fields[grp[1].col]
						field.ref_table = ref_tbl
						field.ref_col = grp[1].ref_col
					end
					attr(tbl, 'fks')[cs_name] = {
						table     = tbl.name,
						ref_table = ref_tbl,
						cols      = imap(grp, row_col),
						ref_cols  = imap(grp, row_ref_col),
						onupdate  = repl(grp[1].onupdate:lower(), 'no action', nil),
						ondelete  = repl(grp[1].ondelete:lower(), 'no action', nil),
					}
					attr(tbl, 'deps')[ref_tbl] = true
				elseif cs_type == 'UNIQUE' then
					attr(tbl, 'uks')[cs_name] = {
						cols = imap(grp, row_col),
					}
				end
			end

		end

		--NOTE: constraints do not create an index if one is already available
		--on the columns that they need, so not every constraint has an entry
		--in the statistics table (which is why we get indexes with another select).
		if opt.all or opt.indexes then
			for i, db_tbl, indices in spp.each_group('db_tbl', self:assert(self:rawquery([[
				select
					concat(s.table_schema, '.', s.table_name) db_tbl,
					s.table_name,
					s.column_name col,
					s.index_name,
					s.collation /* D|A */
				from information_schema.statistics s /* columns for pk, uk, fk, ix */
				left join information_schema.table_constraints cs /* cs type: pk, fk, uk */
					 on cs.table_schema     = s.table_schema
					and cs.table_name       = s.table_name
					and cs.constraint_name  = s.index_name
				where
					cs.constraint_name is null
					and s.table_schema not in ('mysql', 'information_schema', 'performance_schema', 'sys')
					and ]]..(catargs(' and ',
							db  and 's.table_schema = '..sql_db,
							tbl and 's.table_name   = '..sql_tbl) or '1 = 1')..[[
				order by
					s.table_schema, s.table_name
				]])))
			do
				local tbl = tables[db_tbl]
				for i, ix_name, grp in spp.each_group('index_name', indices) do
					local ix = imap(grp, row_col)
					ix.desc = grp[1].collation == 'D' or nil
					attr(tbl, 'ixs')[ix_name] = ix
				end
			end
		end

		if opt.all or opt.checks then
			for i, db_tbl, checks in spp.each_group('db_tbl', self:assert(self:rawquery([[
				select
					concat(cs.table_schema, '.', cs.table_name) db_tbl,
					cc.constraint_name,
					cc.check_clause
				from information_schema.table_constraints cs
				inner join information_schema.check_constraints cc
					 on cs.table_schema    = cc.constraint_schema
					and cs.constraint_name = cc.constraint_name
				where
					cs.table_schema not in ('mysql', 'information_schema', 'performance_schema', 'sys')
					and ]]..(catargs(' and ',
							db  and 'cs.table_schema = '..sql_db,
							tbl and 'cs.table_name   = '..sql_tbl) or '1 = 1')..[[
				order by
					cs.table_schema, cs.table_name
				]])))
			do
				local tbl = tables[db_tbl]
				for i, row in ipairs(checks) do
					attr(tbl, 'checks')[row.constraint_name] = {
						mysql_body = row.check_clause,
					}
				end
			end
		end

		if opt.all or opt.triggers then
			for i, db_tbl, triggers in spp.each_group('db_tbl', self:assert(self:rawquery([[
				select
					concat(event_object_schema, '.', event_object_table) db_tbl,
					trigger_name,
					action_order,
					action_timing,      /* before|after */
					event_manipulation, /* insert|update|delete */
					action_statement
				from information_schema.triggers
				where
					event_object_schema not in ('mysql', 'information_schema', 'performance_schema', 'sys')
					and definer = current_user
					and ]]..(catargs(' and ',
							db  and 'event_object_schema = '..sql_db,
							tbl and 'event_object_table  = '..sql_tbl) or '1 = 1')..[[
				order by
					event_object_schema, event_object_table
				]])))
			do
				local tbl = tables[db_tbl]
				for i, row in ipairs(triggers) do
					attr(tbl, 'triggers')[row.trigger_name] = {
						pos        = row.action_order,
						when       = row.action_timing:lower(),
						op         = row.event_manipulation:lower(),
						mysql_body = row.action_statement,
					}
				end
			end
		end

		return tables
	end

	local function make_param(t)
		local p = field_type_attrs(t)
		p.mode = repl(t.parameter_mode:lower(), 'in', nil)
		p.col  = t.parameter_name --it's weird, but easier bc fields have `col`.
		return p
	end

	--TODO: get functions too.

	function cmd:get_procs(db)
		local procsets = {} --{db->{proc->p}}
		for i, db, procs in spp.each_group('db', self:assert(self:rawquery([[
			select
				r.routine_schema db,
				r.routine_name,
				r.routine_definition,

				p.parameter_mode, /* in|out */
				p.parameter_name,

				/* input for field_type_attrs(): */
				p.data_type,
				p.dtd_identifier column_type,
				p.numeric_precision,
				p.numeric_scale,
				p.ordinal_position,
				p.character_octet_length,
				p.character_set_name,
				p.collation_name,
				p.character_maximum_length

			from information_schema.routines r
			left join information_schema.parameters p
				on p.specific_name = r.routine_name
			where
				r.routine_type = 'PROCEDURE'
				and r.routine_schema <> 'sys'
				]]..(db and ' and r.routine_schema = '..self:sqlval(db) or '')..[[
			order by
				r.routine_schema
			]])))
		do
			local procset = attr(procsets, db)
			for i, proc_name, grp in spp.each_group('routine_name', procs) do
				local p = {
					args       = imap(grp, make_param),
					mysql_body = grp[1].routine_definition,
				}
				procset[proc_name] = p
				for i, param in ipairs(grp) do
					p[i] = self:sqltype(param)
				end
			end
		end
		return procsets
	end

	--DDL commands ------------------------------------------------------------

	spp.default_charset = 'utf8mb4'
	spp.default_collation = 'utf8mb4_unicode_ci'

	--existence tests

	function cmd:schema_exists(name)
		return self:first_row([[
			select 1 from information_schema.schemata
			where schema_name = ?
		]], name or self.db) ~= nil
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
		return cat(code)
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

--TODO: generate these from schema.
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
