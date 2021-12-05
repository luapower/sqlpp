
local spp = require'sqlpp'.new()
require'sqlpp_mysql'
spp.import'mysql'

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
		db = 'sp',
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

	if false then
		pp(cmd:query'delete from val where val = 1')
	end

	if false then
		cmd:insert_rows('val',
			{{note1 = 'x', val1 = 'y'}},
			{note = 'note1', val = 'val1'}
		)
	end

	if true then
		pp(cmd:table_def()['sp.currency'])
	end

end)

