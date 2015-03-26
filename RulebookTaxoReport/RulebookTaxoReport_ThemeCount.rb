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
	
	 tableCheck= ARGV[0]  #Check taxos
	 inputFilename = ARGV[1] #filename of thee include rbid  list file

     currentDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	
	 themes = Hash.new
	 
	 
	 zipFileName = "RulebookTaxoReport_ThemeOnly_#{currentDateTime}.zip"
	 logFileName = "RulebookTaxoReport_ThemeOnly_#{currentDateTime}.log"
	  
	 log = Logger.new(logFileName)
	 
	 #puts "Start Date: #{currentDateTime}  ......"
	 log.debug "Start Date: #{currentDateTime}  ......"
	 
	 #backup output dir from previous run
	 
	 File.rename("ThemeOnly_outputFiles","ThemeOnly_outputFiles_#{currentDateTime}")
	 
	 #puts "creating new directory ..."
	 
	 Dir.mkdir("ThemeOnly_outputFiles")

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
		
		 
	 Dir.chdir ("ThemeOnly_outputFiles")
	 
	 outputThemeCountTaxoFileName = "RulebookTaxoReport_COUNT_ThemeOnly_#{currentDateTime}.csv"
		  
	 outputThemeCountTaxo = File.open("#{outputThemeCountTaxoFileName}","w")
	 
	 outputThemeCountTaxo << ("ThemeName, ThemeID, ThemeCount\r\n")
	 
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
	
		
	     fileRowCount = 0
	
	    if tableCheck == '1' 
	  
         
	      outputThemeTaxoFileName = "RulebookTaxoReport_ThemeOnly_#{rbid}_#{ruleTableNameStrip}_#{currentDateTime}.csv"
		  
	      outputThemeTaxo = File.open("#{outputThemeTaxoFileName}","w")
	 
	      outputThemeTaxo << ("RuleBookName, RuleBookID, RecordId, ElementId, ThemeName, ThemeID\r\n")
			
	     sqlQuery1="select record_id, element_id from rulebooks.#{ruleTableNameStrip} rb where "  + 
	            " rb.record_id NOT in (select DISTINCT(rbLink.record_id) " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink) order by record_id asc" 
				 
		
		 results1 = client.query("#{sqlQuery1}")
		   
		   
		
         results1.each(:as => :hash) do |row1|
	      record = row1['record_id']
		  element = row1['element_id']
		  
	     
		  contentCount =0 
		  orgCount =0 
		  themeCount =0 
		
		  
		  outputThemeTaxo << "#{ruleTableNameStrip}, #{rbid}, #{record},#{element},#{contentCount},#{orgCount},#{themeCount} \r " 
		  
	     end #end  results1.each(:as => :hash) do |row1|

		
		#REcords with taxos AND Only leaf records which are the documents, ie: have no parent
		
		sqlQuery2="select record_id, element_id from rulebooks.#{ruleTableNameStrip} rb where "  + 
	            " rb.record_id in (select DISTINCT(rbLink.record_id) " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink)" +
				" AND rb.record_id NOT IN (select DISTINCT parent from rulebooks.#{ruleTableNameStrip}" +
				" where parent IS NOT NULL) " 
		#		" AND rb.end_date = '0000-00-00'"   #most recent version of the rule

		
		 results2 = client.query("#{sqlQuery2}")
		 
		
        results2.each(:as => :hash) do |row2|
	      record2 = row2['record_id']
		  element2 = row2['element_id']
			   
		   # Get Themes
		   
		       #sqlQuery5="select REPLACE(taxo.name,',',' '), taxo.internal_unique_name " +
			   sqlQuery5="select taxo.name, taxo.internal_unique_name " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink , taxonomy.taxonomy taxo" + 
				" where rbLink.record_id = #{record2} AND " +
				" taxo.id = rbLink.taxonomy_id AND " +
				" taxo.tree_path like '/6378/%' " 
				
			   #puts "#{sqlQuery5}"
			   
			   #exit(0)
			   
		      results5 = client.query("#{sqlQuery5}")
			  
			 
			     #puts "#{sqlQuery5}"
				 
				 #exit(0)
			  
			    results5.each(:as => :hash) do |row5|
	              name5 = row5['name']
		          unique_name5 = row5['internal_unique_name']
		  
		          nameString5=name5.gsub(","," ")

				  
		          outputThemeTaxo  << "#{ruleTableNameStrip}, #{rbid}, #{record2}, #{element2}, #{nameString5},#{unique_name5} \r " 
			      fileRowCount = fileRowCount+1
				  
				  # check if theme already in hash
				  if (themes.has_key? "#{nameString5},#{unique_name5}")
				    
					#puts "in has_key true..."
					
					 #if exists, then get hash value and increment by 1
					
					currentCount = themes["#{nameString5},#{unique_name5}"] 
					currentCount = currentCount+1
					
				    themes["#{nameString5},#{unique_name5}"]= currentCount
					
				  else
				  
				  #puts "in has_key false..."
				  
				    # if not in hash add with a count value of 1
					 themes["#{nameString5},#{unique_name5}"]=1
					
				  end #if (themes.has_key ? #{unique_name5})
			
			     end #results5.each(:as => :hash) do |row5|
				 
				 
				 if (results5.count == 0)
			      outputThemeTaxo  << "#{ruleTableNameStrip}, #{rbid}, #{record2}, #{element2} \r " 
			      fileRowCount = fileRowCount+1
			
			     end #if (results5.count == 0)
			
			   
		    # If we found some themes record
		    #if  (results5.count > 0)
			
			  #outputMissingTaxo  << "#{ruleTableNameStrip}, #{rbid}, #{record2}, #{element2}, #{results3.count},#{results4.count},#{results5.count} \r " 
			  #fileRowCount = fileRowCount+1
			  
			#end #if (results5.count == 0) 
		  
	     end #end results2.each(:as => :hash) do |row2|
		 
		 outputThemeTaxo.close
		 
		 if fileRowCount > 0   #Found data for this rulebook that needs to be recorded
	        resultFiles << outputThemeTaxoFileName
		  
		 end
		 
	   end # end if tableCheck == 1
	 
     end #end results.each(:as => :hash) do |row|
	 
	puts "Before output of hash ..."
		 
    themes.each {|key,value| outputThemeCountTaxo << "#{key},#{value} \r"}
		 
    outputThemeCountTaxo.close
	
	 Dir.chdir ("..")
	  
	 dirDateTime2 = Time.now.strftime("%d%m%Y%H_%M_%S")
	
	 create_zipFile("ThemeOnly_outputFiles/",zipFileName)
	 
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
	
	
	outputThemeTaxo.close
	outputThemeCountTaxo.close
	
	endErrorDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 
	#puts "End Error Date: #{endErrorDateTime}  ......"
	log.error "End Error Date: #{endErrorDateTime}  ......"
	
	send_email2 "barbara.switzer@thomsonreuters.com", "Rulebook Taxo THEME ONLY Run Failed  #{endErrorDateTime}", "Rulebook Taxo Run THEME ONLY Failed...#{endErrorDateTime}", "test.txt"
    
	  
ensure
    client.close if client
end