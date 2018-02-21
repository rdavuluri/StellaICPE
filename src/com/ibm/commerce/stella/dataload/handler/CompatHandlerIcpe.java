//**************************************************************************************************************************
// The purpose of this code is to create WCS7 understandable CSV for CatalogEntryAssociation
// Author:
// Email: 
// Version: 1.0
//**************************************************************************************************************************
package com.ibm.commerce.stella.dataload.handler;

import java.io.*;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import com.ibm.commerce.stella.dataload.form.*;

public class CompatHandlerIcpe extends DefaultHandler {

//	Below are boolean values which will be used if the SAX Parser encounters the corresponding tags
	   boolean bPARTNUMBER = false;
	   boolean bTARGETPARTNUMBER = false;

//	Below value is used for keeping track of the number of element
	   int elementNumber=1;

//	Below are log4j related	   
	   //Logger log = Logger.getLogger(this.getClass());
	   Logger log = Logger.getLogger("compatLogger");
	   String log4JPropertyFile;

//	Below are properties related 
	   String propFileName;
	   String inputFile;
	   String Datesuffix;
	   String outputFile;
	   String ProductBean;
	   String AssociationTypeValue;
	   String QuantityValue;

//	Below are used in SAX Parser for writing the values of the xml tags	   
	   public StringBuffer output = new StringBuffer();
	   public StringBuffer PartNumberBuffer;
	   public StringBuffer TargetPartNumberBuffer; 
	   BufferedWriter out;
	   FileWriter catalogentryassociationfw;


//	Below is for creating object for HandlerBean where all the values of the xml tags will be set	   
	   HandlerBean hb = new HandlerBean();
	   Date date = new Date();



//	The below method startElement is the first method invoked by SAX Parser when we call parse method    
	   //@Override
	   public void startElement(String uri, 
	   String localName, String qName, Attributes attributes)
	      throws SAXException {

	      if (qName.equalsIgnoreCase("WWPARTNUM")) {
	    	  PartNumberBuffer = new StringBuffer();
	    	  bPARTNUMBER = true;
	      } else if (qName.equalsIgnoreCase("CMPOFPNUMB")) {
	    	  TargetPartNumberBuffer = new StringBuffer();
	    	  bTARGETPARTNUMBER = true;
	      }	      
	   }


//	Below is where the actual processing of xml elements starts and the character data is fetched and written to appropriate CSVs 
	   //@Override
	   public void endElement(String uri, 
	   String localName, String qName) throws SAXException {
		   if(qName.equalsIgnoreCase("WWPARTNUM"))
		   {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+PartNumberBuffer.toString().trim());
//	The value is set using setter 			   
			   hb.setPartnumber(PartNumberBuffer.toString().trim());
		     bPARTNUMBER = false;
		   }
		   else if (qName.equalsIgnoreCase("CMPOFPNUMB")) {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+TargetPartNumberBuffer.toString().trim());
			   hb.setTargetPartnumber(TargetPartNumberBuffer.toString().trim());  
			   bTARGETPARTNUMBER = false;
		      } 


	      if (qName.equalsIgnoreCase("OFCMPOF")) {
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          End of element "+elementNumber);
	    	  log.info(" ");
	    	  log.info(" ");
	    	  writeCatalogEntryAssociationCSV();
	    	  elementNumber++;
	      }
	   }

// Below method is used for reading character data of the input xml tags	   
	   //@Override
	   public void characters(char ch[], 
	      int start, int length) throws SAXException {
	      if (bPARTNUMBER) {
	    	  PartNumberBuffer.append(ch, start, length);
	      } else if (bTARGETPARTNUMBER) {
	    	  TargetPartNumberBuffer.append(ch, start, length);
	      } 
	      }


//	writeCatalogEntryAssociationCSV method is used for writing the processed data to the CatalogEntryAssociation.csv file 	   
	   private void writeCatalogEntryAssociationCSV()
	   {
		  output.delete(0, output.length());
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryAssociation.csv");
		  output.append(hb.getPartnumber()+","+AssociationTypeValue+","+hb.getTargetPartnumber()+","+QuantityValue+",");
		  //output.append(System.getProperty("line.separator"));
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
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntryAssociation.csv \n"+e.getMessage());
			}
	   }



//	createOutputFiles method is used for creating headers to CatalogEntryAssociation.csv file	   
	   public void createOutputFiles()
	{
		try
		{
//	Below are for creating CatalogEntryAssociation.csv file			
	    catalogentryassociationfw = new FileWriter(outputFile);
        out = new BufferedWriter(catalogentryassociationfw);
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryAssociation for CatalogEntryAssociation.csv");
        output.append("CatalogEntryAssociation");
        out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntryAssociation.csv");
		output.delete(0, output.length());
		output.append("PartNumber"+","+"AssociationType"+","+"TargetPartNumber"+","+"Quantity"+","+"Delete");
		out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntryAssociation CSV ended");
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
	  //langId = prop.getProperty("langId");
		inputFile = prop.getProperty("inputFile_compat");
	  //store = prop.getProperty("store");
	  //suffix = prop.getProperty("suffix");
		outputFile= prop.getProperty("outputFile_compat");
	  //ItemBean = prop.getProperty("item");
		Datesuffix = prop.getProperty("datesuffix");
		ProductBean = prop.getProperty("product");
		AssociationTypeValue=prop.getProperty("associationtype");
		QuantityValue=prop.getProperty("quantity");
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
