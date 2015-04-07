require 'rubygems'
require 'mysql2'
require 'mail'
require 'zip'
require 'logger'


begin

   
     def create_zipFile(directory,zipfile_name)
  
      Zip::File.open(zipfile_name, Zip::File::CREATE) do |zipfile|
         Dir[File.join(directory, '**', '**')].each do |file|
           zipfile.add(file.sub(directory, ''), file)
		  end
       end
    end
	
	
	def send_email2(to, subject, body,zipFileName)
	 #puts "In send email 2 .."
	 #puts " Body #{body}.."
	 
	 
	 
	   options = { :address              => "mailhub.tfn.com",
            :port                 => 25,
            :domain               => '10.222.138.188',
            :authentication       => 'plain',
            :enable_starttls_auto => true  }
   
	   from = "barbara.switzer@thomsonreuters.com"
		
       Mail.defaults do
           delivery_method :smtp, options
       end


	   Mail.deliver do
          to "#{to}"
          from "#{from}"
          subject "#{subject}"
     	  body "#{body}"
		  #body File.read('test.txt')
		  add_file "#{zipFileName}"
       end
      
	  #puts "Send email2 end..."
	  
    end
	
	 inputFilename = ARGV[0] #filename of the include rbid  list file

     currentDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	
	 zipFileName = "RulebookTaxoReport_FormOnly_#{currentDateTime}.zip"
	 logFileName = "RulebookTaxoReport_FormOnly_#{currentDateTime}.log"
	  
	 log = Logger.new(logFileName)
	 
	 #puts "Start Date: #{currentDateTime}  ......"
	 log.debug "Start Date: #{currentDateTime}  ......"
	 
	 #backup output dir from previous run
	 
	 File.rename("FormOnly_outputFiles","FormOnly_outputFiles_#{currentDateTime}")
	 
	 #puts "creating new directory ..."
	 
	 Dir.mkdir("FormOnly_outputFiles")

    #prod slave
    client = Mysql2::Client.new(:host => "10.198.233.240", :username => "developer", :password => "0rb1tal", :flags => Mysql2::Client::MULTI_STATEMENTS)
   
    sqlQuery=""


	sqlQueryInClause = "("
		
	File.open("#{inputFilename}") do |includedRulesbookFile|
	       includedRulesbookFile.each_line do |line|
		  currentLine = line.strip.sub(/,/,'')
	       sqlQueryInClause = "#{sqlQueryInClause} '#{currentLine }',"
		end #includedRulesbookFile.each_line do |line|
	end #File.open("#{inputFilename}") do |includedRulesbookFile|
		
		 
	 Dir.chdir ("FormOnly_outputFiles")
	 
	 outputFormTaxoFileName = "RulebookTaxoReport_FormOnly_#{currentDateTime}.csv"
		  
	 outputFormTaxo = File.open("#{outputFormTaxoFileName}","w")
	 
	 outputFormTaxo << ("RuleBookName, RuleBookID, RecordId, ElementId\r\n")
	 
	sqlQueryInClause = "#{sqlQueryInClause} )"
	sqlQueryInClause.slice! ", )"
	sqlQueryInClause = "#{sqlQueryInClause} )"
		
	
	
	sqlQuery = "select ref,tablename from rulebooks.rulebook_index where rulebooks.rulebook_index.ref IN #{sqlQueryInClause} order by ref asc"
	
    results = client.query(sqlQuery)
 
	 resultFiles=[] 
	  
	 results.each(:as => :hash) do |row|
	
	   ruleTableName = row['tablename']
	   ruleTableNameStrip = row['tablename']
	   ruleTableNameStrip.strip!
	   
	   rbid = row['ref']
	
		log.debug "Rulebook Name: #{ruleTableNameStrip}, Rulebook ID:  #{rbid} ......\r\n"
		
	     fileRowCount = 0
	
	   
	     sqlQuery1="select record_id, element_id from rulebooks.#{ruleTableNameStrip} rb where "  + 
	            " rb.record_id IN (select DISTINCT(rbLink.record_id) " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink where rbLink.taxonomy_id=8927) order by record_id asc" 
				 
		
		 results1 = client.query("#{sqlQuery1}")
		   	
		 tempCount=results1.count
			
		# if results1.count == 0
		#    outputFormTaxo << "#{ruleTableNameStrip}, #{rbid},,\r " 
		# else
		
           results1.each(:as => :hash) do |row1|
	        record = row1['record_id']
		    element = row1['element_id']
		  
		    outputFormTaxo << "#{ruleTableNameStrip}, #{rbid}, #{record},#{element}\r " 
		  
		  # end #end results1.each(:as => :hash) do |row1|
		   
		 end #if results1.count == 0
		
     end #end results.each(:as => :hash) do |row|
	 
	 outputFormTaxo.close
	
	 Dir.chdir ("..")
	  
	 dirDateTime2 = Time.now.strftime("%d%m%Y%H_%M_%S")
	
	 create_zipFile("FormOnly_outputFiles/",zipFileName)
	 
	 endDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 
	 #puts "End Date: #{endDateTime}  ......"
     log.debug "End Date: #{endDateTime}  ......"
	 
	 #send_email2 "bswitzer91@gmail.com;deborah.johnstone@thomsonreuters.com;jennifer.conway@thomsonreuters.com", "Rulebook Taxo Run Succeeded #{endDateTime}",  "Rulebook Taxo Run Succeeded #{endDateTime} \r\n\r\n  See attached file.. \r\n\r\n  Start Date/Time: #{currentDateTime}  \r\n End Date\Time: #{endDateTime} \r\n\r\n  If you have any questions, please contact me.  -- Barb Switzer 585-627-2398", zipFileName
     #send_email2 "bswitzer91@gmail.com", "Rulebook Taxo THEME ONLY Run Succeeded #{endDateTime}",  "Rulebook Taxo Run THEME ONLY Succeeded #{endDateTime} \r\n\r\n  See attached file.. \r\n\r\n  Start Date/Time: #{currentDateTime}  \r\n End Date\Time: #{endDateTime} \r\n\r\n  If you have any questions, please contact me.  -- Barb Switzer 585-627-2398", zipFileName
   
     log.debug " After Email send ... "
   
rescue Mysql2::Error => e
 
	log.error "Error in job .... "
	
	log.error e.errno
	log.error e.error
	
	outputFormTaxo.close
	
	endErrorDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 
	#puts "End Error Date: #{endErrorDateTime}  ......"
	log.error "End Error Date: #{endErrorDateTime}  ......"
	
	send_email2 "barbara.switzer@thomsonreuters.com", "Rulebook Taxo FORM Only Run Failed  #{endErrorDateTime}", "Rulebook Taxo Run FORM ONLY Failed...#{endErrorDateTime}", "test.txt"
    
	  
ensure
    client.close if client
end