//**************************************************************************************************************************
// The purpose of this code is to create WCS7 understandable CSV for CatalogEntryCalculationCode
// Author:
// Email: 
// Version: 1.0
//**************************************************************************************************************************

package com.ibm.commerce.stella.dataload.handler;

import java.io.*;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import com.ibm.commerce.stella.dataload.form.*;

public class ProductFeeHandlerIcpe extends DefaultHandler {

//	Below are boolean values which will be used if the SAX Parser encounters the corresponding tags
	   boolean bPARTNUMBER = false;
	   boolean bFEECODE = false;
	   
	   boolean duplicate = false;
	   
//	Below value is used for keeping track of the number of element
	   int elementNumber=1;
	   
//	Below are log4j related	   
	   //Logger log = Logger.getLogger(this.getClass());
	   Logger log = Logger.getLogger("productfeeLogger");
	   String log4JPropertyFile;
	   
//	Below are properties related 
	   String propFileName;
	   String Language;
	   String inputFile;
	   String Datesuffix;
	   String Store;
	   String desc;
	   String store;
	   String suffix;
	   String outputFile;
	   String outputFile1;
	   String langId;
	   String ItemBean;
	   String ProductBean;
	   String keyvalue;
	   
//	Below are used in SAX Parser for writing the values of the xml tags	   
	   public StringBuffer output = new StringBuffer();
	   public StringBuffer output1 = new StringBuffer();
	   public StringBuffer PartNumberBuffer;
	   public StringBuffer FeeCodeBuffer; 
	   BufferedWriter out;
	   BufferedWriter out1;
	   FileWriter catalogentrycalculationcodefw;
	   FileWriter catalogentrycalculationcodedupfw;
	   
//	Below is for creating object for HandlerBean where all the values of the xml tags will be set	   
	   HandlerBean hb = new HandlerBean();
	   Date date = new Date();
	   
	   //Map<String, Integer> hashmap = new HashMap<String, Integer>();
	   Map<String, String> hashmap = new HashMap<String, String>();
	   
//	The below method startElement is the first method invoked by SAX Parser when we call parse method    
	   //@Override
	   public void startElement(String uri, 
	   String localName, String qName, Attributes attributes)
	      throws SAXException {
		   
	      if (qName.equalsIgnoreCase("PARTNUMBER")) {
	    	  PartNumberBuffer = new StringBuffer();
	    	  bPARTNUMBER = true;
	      } else if (qName.equalsIgnoreCase("FEECODE")) {
	    	  FeeCodeBuffer = new StringBuffer();
	         bFEECODE = true;
	      } 
	      
	   }

	   
//	Below is where the actual processing of xml elements starts and the character data is fetched and written to appropriate CSVs 
	   //@Override
	   public void endElement(String uri, 
	   String localName, String qName) throws SAXException {
		   if(qName.equalsIgnoreCase("PARTNUMBER"))
		   {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+PartNumberBuffer.toString().trim());
//	The value is set using setter 			   
			   hb.setPartnumber(PartNumberBuffer.toString().trim());
			   bPARTNUMBER = false;
			   
		   }
		   else if (qName.equalsIgnoreCase("FEECODE")) {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+FeeCodeBuffer.toString().trim());
			   hb.setFeecode(FeeCodeBuffer.toString().trim());  
		       bFEECODE = false;
		       
		    // If condition will check whether partnumber  is available in hashmap
			   if(hashmap.containsKey(hb.getPartnumber()))
			   {
				   duplicate = true;
				   keyvalue= hashmap.get(hb.getPartnumber());
				   // If the below condition is true, it will exit the program as Fee code is different for same part number
				   if (!keyvalue.equals(hb.getFeecode()))
				   {
					   /*
					   log.info(" ");
				       log.info(" ");
					   log.error(new Timestamp(date.getTime())+"		ERROR:		Different Fee Code for same part number	 \n");
					   System.exit(1);*/
					   log.info(new Timestamp(date.getTime())+"		DEBUG:			Different Fee Code for same part number	 \n");
					   duplicate = false;
					   hashmap.put(hb.getPartnumber(),hb.getFeecode());
				   }
			   }
			   else
			   {
				   hashmap.put(hb.getPartnumber(),hb.getFeecode());				   
				   duplicate = false;
			   }
		       
		      } 		      
		     

	      if (qName.equalsIgnoreCase("FEEPRODREL")) {
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          End of element "+elementNumber);
	    	  log.info(" ");
	    	  log.info(" ");
	    	  //if(hashmap.containsKey(hb.getPartnumber()))
	    	 // result= hashmap.containsKey(hb.getPartnumber());
	    	  
	    	  if(duplicate)
	    	  {
	    		  writeCatalogEntryCalculationCodeDupCSV();	    		  
	    	  }
	    	  else 
	    	  {
	    		  writeCatalogEntryCalculationCodeCSV();
	    	  }
	    	  elementNumber++;
	      }
	   }

// Below method is used for reading character data of the input xml tags	   
	   //@Override
	   public void characters(char ch[], 
	      int start, int length) throws SAXException {
	      if (bPARTNUMBER) {
	    	  PartNumberBuffer.append(ch, start, length);
	      } else if (bFEECODE) {
	    	 FeeCodeBuffer.append(ch, start, length);
	      } 
	      
	      }


//	writeCatalogEntryCalculationCodeCSV method is used for writing the processed data to the CatalogEntryCalculationCode.csv file 	   
	   private void writeCatalogEntryCalculationCodeCSV()
	   {
		  output.delete(0, output.length());
		  
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryCalculationCode.csv");
		  /* output.append(hb.getPartnumber()+","+hb.getFeecode()+",");
		  output.append(System.getProperty("line.separator")); */
		  output.append(hb.getPartnumber()+suffix+","+hb.getFeecode()+",");
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Data writing completed for element"+elementNumber);
		  log.info(" ");
		  log.info(" ");
		  try
			{
			out.write(output.toString());
			out.newLine();
			out.flush();
			}
			catch(Exception e)
			{
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntryCalculationCode.csv \n"+e.getMessage());
			}
	   }

//		writeCatalogEntryCalculationCodeDupCSV method is used for writing the processed data to the CatalogEntryCalculationCode_dup.csv file 	   
	   private void writeCatalogEntryCalculationCodeDupCSV()
	   {
		  output1.delete(0, output1.length());
		  
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryCalculationCode_dup.csv");
		  /* output1.append(hb.getPartnumber()+","+hb.getFeecode()+",");
		  output1.append(System.getProperty("line.separator"));  */
		  output1.append(hb.getPartnumber()+suffix+","+hb.getFeecode()+",");
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Data writing completed for element"+elementNumber);
		  log.info(" ");
		  log.info(" ");
		  try
			{
			out1.write(output1.toString());
			out1.newLine();
			out1.flush();
			}
			catch(Exception e)
			{
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntryCalculationCode_dup.csv \n"+e.getMessage());
			}
	   }
	   
	   
//	createOutputFiles method is used for creating headers to CatalogEntryCalculationCode.csv file	   
	   public void createOutputFiles()
	{
		try
		{
//	Below are for creating CatalogEntryCalculationCode.csv file			
		catalogentrycalculationcodefw = new FileWriter(outputFile);
        out = new BufferedWriter(catalogentrycalculationcodefw);
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryCalculationCode for CatalogEntryCalculationCode.csv");
        output.append("CatalogEntryCalculationCode");
        out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntryCalculationCode.csv");
		output.delete(0, output.length());
		output.append("PartNumber"+","+"SurchargeCalculationCode"+","+"Delete");
		out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntryCalculationCode CSV ended");
		log.info(" ");
		log.info(" ");		
		}
		catch(Exception e)
		{
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught in createOutputFilesMethod \n"+e.getMessage());
		}
		
		try
		{
//	Below are for creating CatalogEntryCalculationCode_dup.csv file			
		catalogentrycalculationcodedupfw = new FileWriter(outputFile1);
        out1 = new BufferedWriter(catalogentrycalculationcodedupfw);
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryCalculationCode_dup for CatalogEntryCalculationCode_dup.csv");
        output1.append("CatalogEntryCalculationCode");
        out1.write(output1.toString());
		out1.newLine();
		out1.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntryCalculationCode_dup.csv");
		output1.delete(0, output1.length());
		output1.append("PartNumber"+","+"SurchargeCalculationCode"+","+"Delete");
		out1.write(output1.toString());
		out1.newLine();
		out1.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntryCalculationCode_dup CSV ended");
		log.info(" ");
		log.info(" ");		
		}
		catch(Exception e)
		{
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught in createOutputFilesMethod \n"+e.getMessage());
		}
}
	     
	   
	   
//	propertiesRead method is for reading all the values from the properties file 
//	which includes the config file specific to the handler and log4j.properties  	   
public String propertiesRead()
{
	try
	{
		Properties p = new Properties();
		p.load(new FileInputStream(log4JPropertyFile));
		PropertyConfigurator.configure(p);
		Properties prop = new Properties();
		InputStream inputStream;
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Reading data from properties file "+propFileName);
		inputStream = new FileInputStream(propFileName);
		if (inputStream != null) {
		prop.load(inputStream);
	}	
		langId = prop.getProperty("langId");
		inputFile = prop.getProperty("inputFile_productfee");
		store = prop.getProperty("store");
		suffix = prop.getProperty("suffix");
		outputFile= prop.getProperty("outputFile_productfee");
		outputFile1= prop.getProperty("outputFile_productfee_dup");
		ItemBean = prop.getProperty("item");
		Datesuffix = prop.getProperty("datesuffix");
		ProductBean = prop.getProperty("product");
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Property file reading completed");
		log.info(" ");
		log.info(" ");
	} 
catch(Exception e)
{
	 log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while reading properties file "+e.getMessage());
}
return inputFile;
	}


//	setConfigFileName is for setting the names of the config files to be read by the handler 
public void setConfigFileName(String log4JPropertyFile1, String propFileName1)
{
	log4JPropertyFile = log4JPropertyFile1;
	propFileName = propFileName1;
}
}
