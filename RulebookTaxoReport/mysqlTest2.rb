require 'mysql2'


begin
    client = Mysql2::Client.new(:host => "localhost", :username => "root", :password => "Barb8514")
    #puts con.get_server_info
	#result = client.query('CALL sp_customer_list( 25, 10 )')
    rs = client.query('select max(id) from atlas.data')
    puts rs.first   
    
rescue Mysql2::Error => e
    puts e.errno
    puts e.error
    
ensure
    client.close if client
end