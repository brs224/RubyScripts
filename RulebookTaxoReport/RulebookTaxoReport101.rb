require 'mysql2'
require 'net/smtp'
require 'net/ftp'


begin

   
	def send_email(to, subject = "", body = "")
      from = "barbara.switzer@thomsonreuters.com"
      body= "From: #{from}\r\nTo: #{to}\r\nSubject: #{subject}\r\n\r\n#{body}\r\n"

      #Net::SMTP.start('192.168.10.213', 25, '192.168.0.218') do |smtp|
	  Net::SMTP.start('mailhub.tfn.com', 25, '10.222.138.188') do |smtp|
        smtp.send_message body, from, to
      end
    end
	
	
	def say_hello(name)
	   var = "Hello, " + name
	   return var
	end
	
	def ftp_results(resultFiles3)
	
	 dirDateTime2 = Time.now.strftime("%d%m%Y%H_%M_%S")
	 puts "Time now Date: #{dirDateTime2}  ......"
	 
	 ftp=Net::FTP.new
	 ftp.connect('c985rsb.int.westgroup.com',21) 
     ftp.login('tcusr','notwest123') 
	 ftp.passive = true
     ftp.debug_mode = true
	 ftp.read_timeout = 10000


	 ftp.chdir('/home/tcusr/')
	
     ftp.chdir('/home/tcusr/RulebooktaxoReports/') 
	 newDir2= 'RulebookReportRun_' + dirDateTime2
	 
	 ftp.mkdir(newDir2)
	 
	 ftp.sendcmd("SITE CHMOD 7777 #{newDir2}") 
	 
	 ftp.chdir(newDir2) 
	 
	 
	  #puts resultFiles3.inspect
			
	  Dir.chdir ("outputFiles")
	
	  ftp.passive = true
      ftp.debug_mode = true
	  ftp.read_timeout = 10000

      resultFiles3.each do |fileName2|
	    #puts "Filename = #{fileName2}"
	    #puts Dir.pwd
	     #puts "In loop before binary put.."
		 begin
           Timeout.timeout(20) do
             ftp.putbinaryfile("#{fileName2}")
         end
        rescue Timeout::Error
          errors << "File download timed out for: #{fileName2}"
          puts errors.last
        end

	     #ftp.putbinaryfile("#{fileName}") 
         #puts "In loop after binary put..."
	  end
	
	 Dir.chdir ("..")
	
     ftp.close 
	end #ftp_results

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
	 

	 #puts "tableCheck = #{tableCheck}"   # 1= use included table, 0 = use excluded table
	 #puts "skipExcludedRulebooks = #{skipExcludedRulebooks}"   #0 = create excluded table
	 #	 puts "skipExcludedCheck = #{skipExcludedCheck}"   #0 = create excluded table
	#	 	 puts "skipIncluded = #{skipIncluded}"   #0 = create excluded table
	  #puts "inputFilename = #{inputFilename}" # file to use for included or excluded tables
	 #puts "excludedRulebooksRead = #{excludedRulebooksRead}" # file to use for included or excluded tables
	 
     #puts "test test..."
	 #puts say_hello("Barb")
	 
	 # test email send_email
	 
	 #send_email "barbara.switzer@thomsonreuters.com", "test", "blah blah blah"
    	 
	 #exit(0)
	 
     currentDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 puts "Start Date: #{currentDateTime}  ......"
	 
	 #validRulebookCount = 0
	 
	#uat/test
	#client = Mysql2::Client.new(:host => "10.198.229.12", :username => "developer", :password => "0rb1tal")
  
    #prod slave
    client = Mysql2::Client.new(:host => "10.198.233.240", :username => "developer", :password => "0rb1tal", :flags => Mysql2::Client::MULTI_STATEMENTS)
   
    sqlQuery=""
	
    #results = client.query('select ref,tablename from rulebooks.rulebook_index order by tablename asc')
	
	if (skipExcludedCheck == '0') && (skipIncluded == '1') # use sql lookup without rulesbook IN clause
	  sqlQuery = "select ref,tablename from rulebooks.rulebook_index order by tablename asc"
	else  #build sql query with IN clause
	 
	  # if (skipIncluded == 0)  #using included rulebook list
	   
	     #puts" IN if (skipIncluded == 0)"
	   
		#excludedRulebooksRead = "dummy.txt"
		
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
		#puts "SQl query #{sqlQuery} in IF stmt"
	
		#puts "IN skipExcludedRulebooks == 1 ELSE"
	
	    #results = client.query(sqlQuery)  #query only for rulebooks in included list,  input has to be a csv file of just rbid
	 # end
	  
	end #end else skipExcludedCheck
	 
#exit(0)

      #puts "SQl query #{sqlQuery}"
	  
      results = client.query(sqlQuery)
 
	 #outputMissingTaxoFileName = "RulebookTaxoReport_#{currentDateTime}.csv"
	
	 #if (skipExcludedCheck == '0')
	    #excludedRulebooksRead = "ExcludedRulebooks_0302201519_07_30.txt"
	#	excludedRulebooksRead = inputFilename
	
    
	 #end
	 

	 #outputMissingTaxo = File.open("#{outputMissingTaxoFileName}","w")
	 
	 #outputMissingTaxo << ("RuleBookName, RuleBookID, RecordId, ContentTypeCount, OrgCount, ThemeCount\r\n")
	 
	 
	 #excludedRulebooks3 = %w(ACMdemo_taxonomy_link ACETraining ACESalesReports ACENEWSPROMO accounting1399557164767 Abu_Dhabi_Stock_Exchange banking1987 friendly1984 friendly1992 ComplianceLeas inland_revenue insurance1982 nasdaq_virtual_branded sex1986 Lofchies_Supervisory_Responsibilities pensionsact Supervisory_Responsiblities_Matrix HMSO Pension_Schemes)
	 
	 #skipExcludedRulebooks = 0
	 
	 resultFiles=[] 
	 excludedRulebooks3=[] 
	 
	 if (skipExcludedCheck == '0') # skip array setup as file for exclusions not yet created
	   #puts "In skipExcludedCheck..."
	  File.open("#{excludedRulebooksRead}") do |excludedRulesbookFile|
	     excludedRulesbookFile.each_line do |line|
	      excludedRulebooks3 << line.strip
	    end #excludedRulesbookFile.each do |line|
	  end #File.open("excludedRulebooksRead") do |excludedRulesbookFile|

        #excludedRulebooks3.to_s.gsub('"', '')
		#puts excludedRulebooks3.inspect
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
	   #puts "before chgdir outputFiles mainline..."
	   #puts Dir.pwd
	   
	   Dir.chdir ("outputFiles")
	  
	   #puts "after chgdir outputFiles mainline..."
	   #puts Dir.pwd
	 end 
		  
		  
	 results.each(:as => :hash) do |row|
	
	   ruleTableName = row['tablename']
	   ruleTableNameStrip = row['tablename']
	   ruleTableNameStrip.strip!
	   
	   rbid = row['ref']
	    
		#puts "Table name = #{ruleTableNameStrip}"
	
		if skipExcludedRulebooks == '0'  #if ==0 then will create the excluded list of rulebooks for later use
	
        	createExcludedRulebooksList(ruleTableNameStrip,rbid,client,outputrbMissing,outputrbTaxoMissing ,outputrbTaxoEmpty,outputExcludedRulebooks)
		end #end if skipExcludedRulebooks == 0
		 
	   #puts "Table check = #{tableCheck}"
	
	   #tableCheck= 0 #skip over actual taxo queries while we build list of rulebooks to skip
		
		#puts "SkipExcludedCheck = #{skipExcludedCheck}"
		#puts "skipIncluded = #{skipIncluded}"
		#puts "tableCheck = #{tableCheck}"
		
		rulebookExcluded = !excludedRulebooks3.index("#{ruleTableNameStrip}")
		#puts "#{rulebookExcluded}"

	#puts "#{ruleTableNameStrip}"
	
	fileRowCount = 0
		
	   #if ((!excludedRulebooks3.index("#{ruleTableNameStrip}") && skipExcludedCheck == '0') || skipIncluded == '0') && tableCheck == '1'
	    if !excludedRulebooks3.index("#{ruleTableNameStrip}") && tableCheck == '1' &&  ruleTableNameStrip == 'US_CFR17'
	  
          #puts "In non exluded ---> 	  #{ruleTableNameStrip}"
		  #outputMissingTaxoFileName = "./outputFiles/RulebookTaxoReport_#{rbid}_#{ruleTableNameStrip}_#{currentDateTime}.csv"
	      outputMissingTaxoFileName = "RulebookTaxoReport_#{rbid}_#{ruleTableNameStrip}_#{currentDateTime}.csv"
		  
	      outputMissingTaxo = File.open("#{outputMissingTaxoFileName}","w")
	 
	      outputMissingTaxo << ("RuleBookName, RuleBookID, RecordId, ElementId, ContentTypeCount, OrgCount, ThemeCount\r\n")
	 
	 #puts "Created output file ..."
	 #exit(0)
	     # Records with no taxo ids
		 
	     sqlQuery1="select record_id, element_id from rulebooks.#{ruleTableNameStrip} rb where "  + 
	            " rb.record_id NOT in (select DISTINCT(rbLink.record_id) " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink)" 
				

		 results1 = client.query("#{sqlQuery1}")
		   
        results1.each(:as => :hash) do |row1|
	      record = row1['record_id']
		  element = row1['element_id']
		  
	      #puts "#{ruleTableNameStrip}, #{record}, #{element}" 
		  #exit(0)
		  #taxoCount =0 
		  contentCount =0 
		  orgCount =0 
		  themeCount =0 
		  #sectorCount =0 
		  #geographyCount =0 
		  
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
				
				 #puts "#{sqlQuery3}"
		      results3 = client.query("#{sqlQuery3}")
			  
		
			  
		   # Count Orgs
		   
		       sqlQuery4="select rbLink.record_id " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink , taxonomy.taxonomy taxo" + 
				" where rbLink.record_id = #{record2} AND " +
				" taxo.id = rbLink.taxonomy_id AND " +
				" taxo.tree_path like '/6989/%' "
				
				#puts "#{sqlQuery4}"
				
		      results4 = client.query("#{sqlQuery4}")
			  
			
			   
		   # Count Themes
		   
		       sqlQuery5="select rbLink.record_id " +
				" from taxonomy.#{ruleTableNameStrip}_taxonomy_link rbLink , taxonomy.taxonomy taxo" + 
				" where rbLink.record_id = #{record2} AND " +
				" taxo.id = rbLink.taxonomy_id AND " +
				" taxo.tree_path like '/6378/%' " 
				
				#" OR taxo.tree_path like '/5443/%') "   #5443 is Subject, is Subject the same as themes???
				
				#puts "#{sqlQuery5}"
				
		      results5 = client.query("#{sqlQuery5}")
			  
			
			   
			   
		   # If any of the 3 counts = 0 then goes into the file 
		   
		 
		    if (results3.count == 0) || (results4.count == 0) || (results5.count == 0)
			
			  #puts "In zero count..."
		
			  outputMissingTaxo  << "#{ruleTableNameStrip}, #{rbid}, #{record2}, #{element2}, #{results3.count},#{results4.count},#{results5.count} \r " 
			  fileRowCount = fileRowCount+1
			  
			end #if (results3.count == 0) || (results4.count == 0) || (results5.count == 0) 
		  
	     end #end results2.each(:as => :hash) do |row2|
		 
		 outputMissingTaxo.close
		 
		 if fileRowCount == 0
		   puts("File empty .. #{outputMissingTaxoFileName}")
		   #File.delete("#{outputMissingTaxoFileName}")
		 else
		   #puts("File NOT empty .. #{outputMissingTaxoFileName}")
		   resultFiles << outputMissingTaxoFileName
		   # put file in an array so we can ftp later
		 end
		 
	   end # end if !excludedRulebooks2.index(ruleTableNameStrip) && tableCheck == 1
	 
	   #puts "I AM HERE after "
	   
	   #validRulebookCount = validRulebookCount+1
     end #end results.each(:as => :hash) do |row|
	 
	#exit(0)
	 #outputMissingTaxo.close
	
	 
	 if skipExcludedRulebooks == '0'
	   outputrbMissing.close
	   outputrbTaxoMissing.close
	   outputrbTaxoEmpty.close
	   outputExcludedRulebooks.close
	 else
	    Dir.chdir ("..")
	  
	   #puts "after chgdir outputFiles mainline2 end..."
	    #puts Dir.pwd
	 end
	 
	 
	 #ftp all files from array of files that had data for this run
	 #puts "Before FTP routine....."
	 
	 ftp_results(resultFiles)
	 
	 #puts "After FTP routine....."
	 
	 endDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 
	 #puts "validRulebookCount #{validRulebookCount} "
	 puts "End Date: #{endDateTime}  ......"
	
	 send_email "barbara.switzer@thomsonreuters.com", "Rulebook Taxo Run Succeeded #{endDateTime}", "Rulebook Taxo Run Succeeded...#{endDateTime} \r\n\n  Files located at:  \r Hostname: c985rsb.int.westgroup.com, \r Directory: /home/tcusr/RulebooktaxoReports/RulebookReportRun_#{dirDateTime} "
   
rescue Mysql2::Error => e
    puts e.errno
    puts e.error
	
	outputMissingTaxo.close
	
	
	if skipExcludedRulebooks == '0'
	  outputrbMissing.close
	  outputrbTaxoMissing.close
	  outputrbTaxoEmpty.close
      outputExcludedRulebooks.close
	
	 end
	 

	endErrorDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 
	puts "End Error Date: #{endErrorDateTime}  ......"
	
	send_email "barbara.switzer@thomsonreuters.com", "Rulebook Taxo Run Failed  #{endErrorDateTime}", "Rulebook Taxo Run Failed...#{endErrorDateTime}"
    
	  
ensure
    client.close if client
end