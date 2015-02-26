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
	
	def createExcludedRulebooksList(ruleTableName,ruleBookId,client2,outputrbMissing2,outputrbTaxoMissing2,outputrbTaxoEmpty2,outputExcludedRulebooks2)
	
	 
			setDBRulebooks = "use rulebooks"
			setDBRulebooksresults = client2.query("#{setDBRulebooks}")
			
			tableChecksqlQuery ="SHOW TABLES LIKE '#{ruleTableName}'"
			#puts "tableChecksqlQuery: #{tableChecksqlQuery}"
			tableChecksqlresults = client2.query("#{tableChecksqlQuery}")
			   
			tempCount=tableChecksqlresults.count
			#puts "Check count 1 = #{tempCount}"
			 
			 if tableChecksqlresults.count == 0
				outputrbMissing2 << "rulebooks.#{ruleTableName}, #{ruleBookId}\r\n " 
				outputExcludedRulebooks2 <<  "#{ruleTableName}\r\n"
				
				#puts "Rulebook missing rulebooks.#{ruleTableName}, #{ruleBookId}"
				tableCheck = 0;
				#puts "In first table check, tableCheck = #{tableCheck}"
			 else
			   #puts "check taxo tables"
			   #puts "In second table check, tableCheck = #{tableCheck}"
			   setDBtaxonomy = "use taxonomy"
			   setDBtaxonomyresults = client2.query("#{setDBtaxonomy}")
			
			   tableChecksqlQuery1 ="SHOW TABLES LIKE '#{ruleTableName}_taxonomy_link'"
			   #puts "tableChecksqlQuery1: #{tableChecksqlQuery1}"
			   tableChecksqlresults1 = client2.query("#{tableChecksqlQuery1}")
			
			   if (tableChecksqlresults1.count == 0)
				outputrbTaxoMissing2 << "taxonomy.#{ruleTableName}_taxonomy_link, #{ruleBookId}\r" 
				outputExcludedRulebooks2 << "#{ruleTableName}\r\n"
				#puts "Rulebook taxo link missing taxonomy.#{ruleTableName}_taxonomy_link #{ruleBookId}"
				tableCheck = 0;
			   else
				 tableChecksqlQuery2 ="Select * from taxonomy.#{ruleTableName}_taxonomy_link"
				 #puts "tableChecksqlQuery2: #{tableChecksqlQuery2}"
				 tableChecksqlresults2 = client2.query("#{tableChecksqlQuery2}")
			
				 if (tableChecksqlresults2.count == 0)
				   outputrbTaxoEmpty2 << "taxonomy.#{ruleTableName}_taxonomy_link, #{ruleBookId}\r\n " 
				   outputExcludedRulebooks2 << "#{ruleTableName}\r\n"
				   #puts "Rulebook taxo table empty.#{ruleTableName}_taxonomy_link #{ruleBookId}"
				   tableCheck = 0;
			   
				 end #end tableChecksqlresults2.count == 0
				end #end else if (tableChecksqlresults1.count == 0)
			 end #end else if tableChecksqlresults.count == 0
	
	end #createExcludedRulebooksList
	
	
	 tableCheck= ARGV[0]  #Check taxos
	 skipExcludedRulebooks= ARGV[1]  #Skip creation of excluded rulebook list
	 skipExcludedCheck= ARGV[2] #Skip use of excluded rulebook list
	 skipIncluded= ARGV[3] #If = 0 , then use included list of rulebooks
	 inputFilename = ARGV[4] #filename of thee include rbid  list file
	 excludedRulebooksRead = ARGV[5] #filename of excluded rb table names
	 

	 
	
     #send_email2 "barbara.switzer@thomsonreuters.com", "test", "blah blah blah", "test.txt"
    
	 #exit(0)
	 
     currentDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	
	 
	 zipFileName = "RulebookTaxoReport_#{currentDateTime}.zip"
	 logFileName = "RulebookTaxoReport_#{currentDateTime}.log"
	  
	 log = Logger.new(logFileName)
	 
	 #puts "Start Date: #{currentDateTime}  ......"
	 log.debug "Start Date: #{currentDateTime}  ......"
	 
	 #backup output dir from previous run
	 
	 File.rename("outputFiles","outputFiles_#{currentDateTime}")
	 
	 #puts "creating new directory ..."
	 
	 Dir.mkdir("outputFiles")

    #prod slave
    client = Mysql2::Client.new(:host => "10.198.233.240", :username => "developer", :password => "0rb1tal", :flags => Mysql2::Client::MULTI_STATEMENTS)
   
    sqlQuery=""

	if (skipExcludedCheck == '0') && (skipIncluded == '1') # use sql lookup without rulesbook IN clause
	  sqlQuery = "select ref,tablename from rulebooks.rulebook_index order by tablename asc"
	else  #build sql query with IN clause
	 

		
		sqlQueryInClause = "("
		
		 File.open("#{inputFilename}") do |includedRulesbookFile|
	     includedRulesbookFile.each_line do |line|
		   currentLine = line.strip.sub(/,/,'')
	      sqlQueryInClause = "#{sqlQueryInClause} '#{currentLine }',"
		  end #includedRulesbookFile.each_line do |line|
		 end #File.open("#{inputFilename}") do |includedRulesbookFile|
		
		
		sqlQueryInClause = "#{sqlQueryInClause} )"
		sqlQueryInClause.slice! ", )"
		sqlQueryInClause = "#{sqlQueryInClause} )"
		
	
		sqlQuery = "select ref,tablename from rulebooks.rulebook_index where rulebooks.rulebook_index.ref IN #{sqlQueryInClause} order by ref asc"
	
	  
	end #end else skipExcludedCheck

      results = client.query(sqlQuery)
 

	 resultFiles=[] 
	 excludedRulebooks3=[] 
	 
	 if (skipExcludedCheck == '0') # skip array setup as file for exclusions not yet created
	   #puts "In skipExcludedCheck..."
	  File.open("#{excludedRulebooksRead}") do |excludedRulesbookFile|
	     excludedRulesbookFile.each_line do |line|
	      excludedRulebooks3 << line.strip
	    end #excludedRulesbookFile.each do |line|
	  end #File.open("excludedRulebooksRead") do |excludedRulesbookFile|

	 end #end skipExcludedRulebooks
	 
	
	
	 if skipExcludedRulebooks == '0' #open output files, write headers
	  rbMissingoutputFileName = "./outputFiles/RulebookMissingReport_#{currentDateTime}.csv"
	  rbTaxoMissingoutputFileName = "./outputFiles/RulebookTaxoMissingReport_#{currentDateTime}.csv"
	  rbTaxoEmptyoutputFileName = "./outputFiles/RulebookTaxoEmptyReport_#{currentDateTime}.csv"
	  excludedRulebooks = "ExcludedRulebooks_#{currentDateTime}.txt"
	 
	  #puts "Creating File #{outputFileName}"
	 
	  outputrbMissing = File.open("#{rbMissingoutputFileName}","w")
	  outputrbTaxoMissing = File.open("#{rbTaxoMissingoutputFileName}","w")
	  outputrbTaxoEmpty = File.open("#{rbTaxoEmptyoutputFileName}","w")
	   outputExcludedRulebooks = File.open("#{excludedRulebooks}","w")
	 
	  outputrbMissing << ("RuleBookName, RuleBookID\r\n")
	  outputrbTaxoMissing << ("RuleBookName, RuleBookID\r\n")
	  outputrbTaxoEmpty << ("RuleBookName, RuleBookID\r\n")
	  
	  
	 end  #skipExcludedRulebooks == 0, open output files, write headers
	 
	 if skipExcludedRulebooks == '1'
	
	   
	   Dir.chdir ("outputFiles")
	  
	 end 
		  
		  
	 results.each(:as => :hash) do |row|
	
	   ruleTableName = row['tablename']
	   ruleTableNameStrip = row['tablename']
	   ruleTableNameStrip.strip!
	   
	   rbid = row['ref']
	
		if skipExcludedRulebooks == '0'  #if ==0 then will create the excluded list of rulebooks for later use
	
        	createExcludedRulebooksList(ruleTableNameStrip,rbid,client,outputrbMissing,outputrbTaxoMissing ,outputrbTaxoEmpty,outputExcludedRulebooks)
		end #end if skipExcludedRulebooks == 0
	
		rulebookExcluded = !excludedRulebooks3.index("#{ruleTableNameStrip}")

	     fileRowCount = 0
	
	    if !excludedRulebooks3.index("#{ruleTableNameStrip}") && tableCheck == '1' #&&  ruleTableNameStrip == 'US_CFR17'
	  
         
	      outputMissingTaxoFileName = "RulebookTaxoReport_#{rbid}_#{ruleTableNameStrip}_#{currentDateTime}.csv"
		  
	      outputMissingTaxo = File.open("#{outputMissingTaxoFileName}","w")
	 
	      outputMissingTaxo << ("RuleBookName, RuleBookID, RecordId, ElementId, ContentTypeCount, OrgCount, ThemeCount\r\n")
	 
	
		
	     sqlQuery1="select record_id, element_id from rulebooks.#{ruleTableNameStrip} rb where "  + 
	            " rb.record_id NOT in (select DISTINCT(rbLink.record_id) " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink)" 
				 
		
		 results1 = client.query("#{sqlQuery1}")
		   
		   
		
        results1.each(:as => :hash) do |row1|
	      record = row1['record_id']
		  element = row1['element_id']
		  
	     
		  contentCount =0 
		  orgCount =0 
		  themeCount =0 
		
		  
		  outputMissingTaxo << "#{ruleTableNameStrip}, #{rbid}, #{record},#{element},#{contentCount},#{orgCount},#{themeCount} \r " 
		  
	     end #end  results1.each(:as => :hash) do |row1|

	
		 # REcords with taxos
		 
		 sqlQuery2="select record_id, element_id from rulebooks.#{ruleTableNameStrip} rb where "  + 
	            " rb.record_id in (select DISTINCT(rbLink.record_id) " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink)" +
				" AND rb.end_date = '0000-00-00'"   #most recent version of the rule
				

		 results2 = client.query("#{sqlQuery2}")
		 
		
		   
        results2.each(:as => :hash) do |row2|
	      record2 = row2['record_id']
		  element2 = row2['element_id']
	   
	       # Count Content Types
		   
		      sqlQuery3="select rbLink.record_id " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink , taxonomy.taxonomy taxo" + 
				" where rbLink.record_id = #{record2} AND " +
				" taxo.id = rbLink.taxonomy_id AND " +
				" taxo.tree_path like '/8884/%' " 
				
		
		      results3 = client.query("#{sqlQuery3}")
			  
	
		   # Count Orgs
		   
		       sqlQuery4="select rbLink.record_id " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink , taxonomy.taxonomy taxo" + 
				" where rbLink.record_id = #{record2} AND " +
				" taxo.id = rbLink.taxonomy_id AND " +
				" taxo.tree_path like '/6989/%' "
	
		      results4 = client.query("#{sqlQuery4}")
			  

			   
		   # Count Themes
		   
		       sqlQuery5="select rbLink.record_id " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink , taxonomy.taxonomy taxo" + 
				" where rbLink.record_id = #{record2} AND " +
				" taxo.id = rbLink.taxonomy_id AND " +
				" taxo.tree_path like '/6378/%' " 
				
			
		      results5 = client.query("#{sqlQuery5}")
			  
			
		 
		    if (results3.count == 0) || (results4.count == 0) || (results5.count == 0)
			
			  outputMissingTaxo  << "#{ruleTableNameStrip}, #{rbid}, #{record2}, #{element2}, #{results3.count},#{results4.count},#{results5.count} \r " 
			  fileRowCount = fileRowCount+1
			  
			end #if (results3.count == 0) || (results4.count == 0) || (results5.count == 0) 
		  
	     end #end results2.each(:as => :hash) do |row2|
		 
		 outputMissingTaxo.close
		 
		 if fileRowCount > 0   #Found data for this rulebook that needs to be recorded
	        resultFiles << outputMissingTaxoFileName
		  
		 end
		 
	   end # end if !excludedRulebooks2.index(ruleTableNameStrip) && tableCheck == 1
	 
     end #end results.each(:as => :hash) do |row|
	 
	
	 
	 if skipExcludedRulebooks == '0'
	   outputrbMissing.close
	   outputrbTaxoMissing.close
	   outputrbTaxoEmpty.close
	   outputExcludedRulebooks.close
	 else
	    Dir.chdir ("..")
	  
	 
	 end
	 
	
	 dirDateTime2 = Time.now.strftime("%d%m%Y%H_%M_%S")
	
	 create_zipFile("outputFiles/",zipFileName)
	 
	 endDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 
	 #puts "End Date: #{endDateTime}  ......"
     log.debug "End Date: #{endDateTime}  ......"
	 
	 send_email2 "bswitzer91@gmail.com;deborah.johnstone@thomsonreuters.com;jennifer.conway@thomsonreuters.com", "Rulebook Taxo Run Succeeded #{endDateTime}",  "Rulebook Taxo Run Succeeded #{endDateTime} \r\n\r\n  See attached file.. \r\n\r\n  Start Date/Time: #{currentDateTime}  \r\n End Date\Time: #{endDateTime} \r\n\r\n  If you have any questions, please contact me.  -- Barb Switzer 585-627-2398", zipFileName
     #send_email2 "bswitzer91@gmail.com", "Rulebook Taxo Run Succeeded #{endDateTime}",  "Rulebook Taxo Run Succeeded #{endDateTime} \r\n\r\n  See attached file.. \r\n\r\n  Start Date/Time: #{currentDateTime}  \r\n End Date\Time: #{endDateTime} \r\n\r\n  If you have any questions, please contact me.  -- Barb Switzer 585-627-2398", zipFileName
   
     log.debug " After Email send ... "
   
rescue Mysql2::Error => e
    #puts e.errno
    #puts e.error
	
	log.error "Error in job .... "
	
	log.error e.errno
	log.error e.error
	
	
	outputMissingTaxo.close
	
	
	if skipExcludedRulebooks == '0'
	  outputrbMissing.close
	  outputrbTaxoMissing.close
	  outputrbTaxoEmpty.close
      outputExcludedRulebooks.close
	
	 end
	 

	endErrorDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 
	#puts "End Error Date: #{endErrorDateTime}  ......"
	log.error "End Error Date: #{endErrorDateTime}  ......"
	
	send_email2 "barbara.switzer@thomsonreuters.com", "Rulebook Taxo Run Failed  #{endErrorDateTime}", "Rulebook Taxo Run Failed...#{endErrorDateTime}", "test.txt"
    
	  
ensure
    client.close if client
end