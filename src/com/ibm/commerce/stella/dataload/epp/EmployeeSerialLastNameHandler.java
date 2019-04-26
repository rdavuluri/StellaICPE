package com.ibm.commerce.stella.dataload.epp;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class EmployeeSerialLastNameHandler {
	private static String field1 = null;
	private static String field2 = null;
	private static String field3 = null;
	private static String s = null;
	private static String propFileName=null;
	private static String inputFile1 =null;
	private static String inputFile2 =null;
	private static String outputFile1 =null;
	private static String outputFile2 =null;
	private static String log4JPropertyFile = null;
	private static String delete = null;
	private static String firstCharacter = null;
	static Logger log= Logger.getLogger("eppHandlerLogger");	   
	static Date date = new Date();
	private static String storeId = null;
	public void globalRead()
	{
		try
		{
			Properties p = new Properties();
			p.load(new FileInputStream(log4JPropertyFile));
			PropertyConfigurator.configure(p);
			Properties prop = new Properties();
			InputStream inputStream;
			log.info(new Timestamp(date.getTime())+"		INFORMATION:	Reading data from GlobalEPP file "+propFileName);
			inputStream = new FileInputStream(propFileName);
			if (inputStream != null) {
			prop.load(inputStream);
			storeId = prop.getProperty("StoreID");	
			inputFile1 = prop.getProperty("INPUT");			
			inputFile2 = prop.getProperty("INPUTDELETE");			
			outputFile1 = prop.getProperty("OUTPUT");
			outputFile2 = prop.getProperty("OUTPUTDELETE");
			delete = prop.getProperty("delete");
			log.info(new Timestamp(date.getTime())+"		INFORMATION:	Property file reading completed");
			log.info(" ");
			log.info(" ");
			}	
		} 
		catch(Exception e)
		{
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while reading properties file "+e.getMessage());
		}
	}			
	public void processData(String inputfile, String outputfile)
	{
		try {
			FileReader fin=new FileReader(inputfile);
			BufferedReader reader = new BufferedReader(fin);
			FileWriter fw=new FileWriter(outputfile); 
			BufferedWriter out = new BufferedWriter(fw);
			log.info(new Timestamp(date.getTime())+"		INFORMATION:	Headers are being written");
			out.write("XicRestrictAuth");
	   	 	out.newLine();
	   	 	out.write("storeId,field1,field2,field3,lastUpdate,Delete");
	   	 	out.newLine();
	   	 	out.flush();
	   	 	log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing of headers completed");
	   	 	log.info(" ");
	   	 	log.info(new Timestamp(date.getTime())+"		INFORMATION:	Actual processing of data starts");	
	   	 	
	   	 	while ((s= reader.readLine())!=null)
				{
	   	 			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String timestamp  = dateFormat.format(new Date());
					Scanner s1 = new Scanner(s).useDelimiter("\\s+");
					s1.next();
					field1 = s1.next();  
				    field2 = s1.next();
				    	firstCharacter = String.valueOf(field2.charAt(0));
						if( firstCharacter.equals("0") && field2.length()>6)
						{
							field2=field2.substring(1);							
						}
					field3 = s1.next();
					
				    if(inputfile==inputFile1){ 
				    	out.write(storeId+","+field1+","+field2+","+field3+","+timestamp+",");
				    	out.newLine();
			    		out.flush();
				    }
				    else if(inputfile==inputFile2){ 
					    out.write(storeId+","+field1+","+field2+","+field3+","+timestamp+","+delete);
					    out.newLine();
				    	out.flush();
					}
				    				    
				 }
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Actual processing of data ends");
		log.info(" ");
		fin.close();
		fw.close();
		reader.close();
		out.close();
		}		
		catch (Exception e) {
			
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while processing input file "+e.getMessage());
					e.printStackTrace();
		}
	}
	public static void main(String args[]){
	propFileName = "C:/Users/SaiKumarVemula/sai/StellaICPE/eppconf/GlobalEpp.ini";
	log4JPropertyFile = "C:/Users/SaiKumarVemula/sai/StellaICPE/eppconf/log4j.properties";
	//propFileName = args[0];
	//log4JPropertyFile=args[1];
	EmployeeSerialLastNameHandler emp = new EmployeeSerialLastNameHandler();
	emp.globalRead();
	File file=new File(inputFile2);
	if (file.exists()) 
    {
    	if (file.canRead())
    	{
    		emp.processData(inputFile1,outputFile1);
    		emp.processData(inputFile2,outputFile2);
    	}
    	else
    	{
    		log.error(new Timestamp(date.getTime())+"		ERROR:	The file "+file+" is not readable");
	        System.exit(2);	
    	}
    }
	else
	{
		log.info(new Timestamp(date.getTime())+"		INFORMATION:    Input delete file doesnot exists so processing only input file. 	");
		emp.processData(inputFile1,outputFile1);					
	}
	
}
}