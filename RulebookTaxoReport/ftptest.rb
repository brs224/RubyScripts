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
	
	 
	# puts resultFiles.inspect
	 
	 dirDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 puts "Time now Date: #{dirDateTime}  ......"
	 
	 ftp=Net::FTP.new
	 ftp.connect('c985rsb.int.westgroup.com',21) 
     ftp.login('tcusr','notwest123') 
	 ftp.passive = true
     ftp.debug_mode = true
	 ftp.read_timeout = 10000


	 ftp.chdir('/home/tcusr/')
	 files = ftp.list
	 puts "File list 1..."
	 puts files
     ftp.chdir('/home/tcusr/RulebooktaxoReports/') 
	 newDir= 'RulebookReportRun_' + dirDateTime
	 puts "New Dir ->  #{newDir}"
	 
	 ftp.mkdir(newDir)
	 
	 ftp.sendcmd("SITE CHMOD 7777 #{newDir}") 
	 
	  files2 = ftp.list
	 puts "File list 2..."
	 puts files2

	 ftp.chdir(newDir) 
	 
	  files3 = ftp.list
	 puts "File list 3..."
	 puts files3
	 

	 resultFiles = ["RulebookTaxoReport_1160_US_CFR17_1202201516_15_18.csv"]
	 
	 puts "Output files...."
	 
	  puts resultFiles.inspect
			
	  puts "Before for each ..."
			
	  puts "before chgdir outputFiles..."
	  puts Dir.pwd
	  
	  Dir.chdir ("outputFiles")
	  
	  puts "after chgdir outputFiles..."
	  puts Dir.pwd
	  
	  puts "List of files in dir..."
	  puts Dir.entries(".")
	  puts "After dir listing .."
	  
	  ftp.passive = true
      ftp.debug_mode = true
	  ftp.read_timeout = 10000

      resultFiles.each do |fileName|
	    puts "Filename = #{fileName}"
	    puts Dir.pwd
	     puts "In loop before binary put.."
		 begin
           Timeout.timeout(20) do
             ftp.putbinaryfile("#{fileName}")
         end
        rescue Timeout::Error
          errors << "File download timed out for: #{fileName}"
          puts errors.last
        end

	     #ftp.putbinaryfile("#{fileName}") 
         puts "In loop after binary put..."
	  end
	
puts "After loop.."
	 Dir.chdir ("..")
	puts "After chdir ..."
	puts Dir.pwd
	
	puts "After for each ..."
	 
     ftp.close 


	 endDateTime = Time.now.strftime("%d%m%Y%H_%M_%S")
	 
	 #puts "validRulebookCount #{validRulebookCount} "
	 puts "End Date: #{endDateTime}  ......"
	
	 send_email "barbara.switzer@thomsonreuters.com", "Rulebook Taxo Run Succeeded #{endDateTime}", "Rulebook Taxo Run Succeeded...#{endDateTime} \r\n\n  Files located at:  \r Hostname: c985rsb.int.westgroup.com, \r Directory: /home/tcusr/RulebooktaxoReports/RulebookReportRun_#{dirDateTime} "

end
