package com.ibm.commerce.stella.dataload.accesscode;

import java.io.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class AccessCode {
	//private static String field1;
	private static String field2;
	private static String field3;
	private static String propFileName=null;
	private static String inputFile1 =null;
	private static String inputFile2 =null;
	private static String outputFile1 =null;
	private static String outputFile2 =null;
	private static String log4JPropertyFile = null;
	private static String delete = null;
	static Logger log= Logger.getLogger("xicRestrictAuthLogger");	   
	static Date date = new Date();
	private static String storeId = null;
	private static String storeId_US = null;
	private static String storeField = null;
	private static String storeField_US = null;
	private static String storeId_CA = null;
	private static String storeField_CA = null;
	
	public static void main(String[] args) {
		
		propFileName = "C:/Users/SaiKumarVemula/sai/StellaICPE/conf_cpp/GlobalCpp.ini";
		log4JPropertyFile = "C:/Users/SaiKumarVemula/sai/StellaICPE/conf_cpp/log4j.properties";
		//propFileName = args[0];
		//log4JPropertyFile = args[1];
		AccessCode ac = new AccessCode();
		ac.globalRead();
		File file=new File(inputFile2);
		if (file.exists()) 
	    {
	    	if (file.canRead())
	    	{
	    	ac.accessFile(inputFile1,outputFile1);
	    	ac.accessFile(inputFile2,outputFile2);
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
			ac.accessFile(inputFile1,outputFile1);					
		}
//	************************************************
//  Reading Accesscode_ICPENA input file.		
		}
	
	public void globalRead(){
		try
		{
			Properties p = new Properties();
			p.load(new FileInputStream(log4JPropertyFile));
			PropertyConfigurator.configure(p);
			Properties prop = new Properties();
			InputStream inputStream;
			log.info(new Timestamp(date.getTime())+"		INFORMATION:	Reading data from Global_xicRestrcitAuth file "+propFileName);
			inputStream = new FileInputStream(propFileName);
			if (inputStream != null) {
			prop.load(inputStream);
			storeId_US = prop.getProperty("StoreID_US");				
			storeField_US = prop.getProperty("store_field1_US");
			storeId_CA = prop.getProperty("StoreID_CA");				
			storeField_CA = prop.getProperty("store_field1_CA");
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
	
	public void accessFile(String inputfile, String outputfile){
		
		try{
			
		String line = null;
		FileReader in = new FileReader(inputfile);
		BufferedReader br = new BufferedReader(in);
		FileWriter out = new FileWriter(outputfile);
		BufferedWriter bw = new BufferedWriter(out);
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Headers are being written");
		bw.write("XicRestrictAuth");
   	 	bw.newLine();
   	 	bw.write("storeId,field1,field2,field3,lastUpdate,Delete");
   	 	bw.newLine();
   	 	bw.flush();
   	 	
   	    log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing of headers completed");
	 	log.info(new Timestamp(date.getTime())+"		INFORMATION:	Actual processing of data starts");
	 	
		while((line = br.readLine())!=null){
			
			if(line.contains("#")){
//				bw.newLine();
				bw.flush();
			}
			else{
		    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");		
		    String timestamp = dateFormat.format(new Date());
			Scanner s = new Scanner(line).useDelimiter(",");
			s.next();
			field2 = s.next();
			if ( field2.equals("US_CPP"))
			{
				storeId = storeId_US;
				storeField = storeField_US;
			}
			else if ( field2.equals("CA_CPP"))
			{
				storeId = storeId_CA;
				storeField = storeField_CA;
			}
			else
			{
				//System.out.println(field2+" : Please check the store field ");
				log.error(new Timestamp(date.getTime())+"		ERROR:	Please check the store field  "+field2);
	    		System.exit(1);
			}
			field3 = s.next();
			if(inputfile==inputFile1){
				
			bw.write(storeId+","+storeField+",#"+field3+","+field3+","+timestamp+",");
			bw.newLine();
			bw.flush();
			
			}
			else if(inputfile==inputFile2){
				
			bw.write(storeId+","+storeField+",#"+field3+","+field3+","+timestamp+","+delete);
			bw.newLine();
			bw.flush();		
			}
			}
		}
	    log.info(new Timestamp(date.getTime())+"		INFORMATION:    File processing completed. 	");
		in.close();
		out.close();
		br.close();
		bw.close();
		}	
		
		catch (Exception e) {
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while processing input file "+e.getMessage());
			e.printStackTrace();
		}

	}
	
}