//**************************************************************************************************************************
// The purpose of this code is to create WCS7 understandable CSV for CatalogEntryOfferPrice
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

public class PriceHandlerIcpe extends DefaultHandler {

//	Below are boolean values which will be used if the SAX Parser encounters the corresponding tags
	   boolean bPARTNUMBER = false;
	   boolean bPRICE = false;
	   boolean bCURRENCY = false;
	   boolean bSTARTDATE = false;
	   boolean bSTOPDATE = false;
	   boolean bPRECEDENCE = false;
	   boolean bCOUNTRY = false;

	   
//	Below value is used for keeping track of the number of element
	   int elementNumber=1;
	   
//	Below are log4j related	   
	   //Logger log = Logger.getLogger(this.getClass());
	   Logger log = Logger.getLogger("priceLogger");
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
	   
	   
//	Below are used in SAX Parser for writing the values of the xml tags	   
	   public StringBuffer output = new StringBuffer();
	   public StringBuffer output1 = new StringBuffer();
	   public StringBuffer PartNumberBuffer;
	   public StringBuffer PriceBuffer; 
	   public StringBuffer CurrencyBuffer;
	   public StringBuffer StartDateBuffer;
	   public StringBuffer StopDateBuffer;
	   public StringBuffer PrecedenceBuffer;
	   public StringBuffer CountryBuffer;
	   BufferedWriter out;
	   BufferedWriter out1;
	   FileWriter catalogentryofferpricefw;
	   FileWriter catentofferpricelistfw;
	   
	   
//	Below is for creating object for HandlerBean where all the values of the xml tags will be set	   
	   HandlerBean hb = new HandlerBean();
	   Date date = new Date();
	   
	   
	   
//	The below method startElement is the first method invoked by SAX Parser when we call parse method    
	   //@Override
	   public void startElement(String uri, 
	   String localName, String qName, Attributes attributes)
	      throws SAXException {
		   
	      if (qName.equalsIgnoreCase("PARTNUMBER")) {
	    	  PartNumberBuffer = new StringBuffer();
	    	  bPARTNUMBER = true;
	      } else if (qName.equals("PRICE")) {
	    	  PriceBuffer = new StringBuffer();
	         bPRICE = true;
	      } else if (qName.equalsIgnoreCase("CURRENCY")) {
	    	  CurrencyBuffer = new StringBuffer();
	         bCURRENCY = true;
	      }
	      else if (qName.equalsIgnoreCase("STARTDATE")) {
	    	  StartDateBuffer = new StringBuffer();
	    		  bSTARTDATE = true;
	    		  }
	      else if (qName.equalsIgnoreCase("STOPDATE")) {
	    	  StopDateBuffer = new StringBuffer();
    		  bSTOPDATE = true;
	   }
	      else if (qName.equalsIgnoreCase("PRECEDENCE")) {
	    	  PrecedenceBuffer = new StringBuffer();
    		  bPRECEDENCE = true;
	   }
	      else if (qName.equalsIgnoreCase("COUNTRY")) {
	    	  CountryBuffer = new StringBuffer();
    		  bCOUNTRY = true;
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
		   else if (qName.equals("PRICE")) {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+PriceBuffer.toString().trim());
			   hb.setPrice(PriceBuffer.toString().trim());  
		         bPRICE = false;
		      } else if (qName.equalsIgnoreCase("CURRENCY")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+CurrencyBuffer.toString().trim());
		    	  hb.setCurrency(CurrencyBuffer.toString().trim());
		         bCURRENCY = false;
		      }
		      else if (qName.equalsIgnoreCase("STARTDATE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+StartDateBuffer.toString().trim());
//		    		The date received from upstream is converted to WCS7 understandable format below		    	  
		    	  StartDateBuffer.append(" "+Datesuffix);
		    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Added date suffix "+Datesuffix);
		    	  hb.setAnnouncedate(StartDateBuffer.toString().trim());
		         bSTARTDATE = false;
		      }
		      else if (qName.equalsIgnoreCase("STOPDATE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+StopDateBuffer.toString().trim());
//		    		The date received from upstream is converted to WCS7 understandable format below		    	  
		    	  StopDateBuffer.append(" "+Datesuffix);
		    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Added date suffix "+Datesuffix);
		    	  hb.setWithdrawdate(StopDateBuffer.toString().trim());
		    	  bSTOPDATE = false;
		      }
		      
		      else if (qName.equalsIgnoreCase("PRECEDENCE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+PrecedenceBuffer.toString().trim());
		    	  hb.setPrecedence(PrecedenceBuffer.toString().trim());
	    		  bPRECEDENCE = false;
		   }
		      else if (qName.equalsIgnoreCase("COUNTRY")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+CountryBuffer.toString().trim());
		    	  hb.setCountry(CountryBuffer.toString().trim());
	    		  bCOUNTRY = false;
		   }
		     

	      if (qName.equals("Price")) {
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          End of element "+elementNumber);
	    	  log.info(" ");
	    	  log.info(" ");
	    	  writeCatalogEntryOfferPriceCSV();
	    	  writeCatalogEntryOfferPriceListCSV();
	    	  elementNumber++;
	      }
	   }

// Below method is used for reading character data of the input xml tags	   
	   //@Override
	   public void characters(char ch[], 
	      int start, int length) throws SAXException {
	      if (bPARTNUMBER) {
	    	  PartNumberBuffer.append(ch, start, length);
	      } else if (bPRICE) {
	    	 PriceBuffer.append(ch, start, length);
	      } else if (bCURRENCY) {
	    	  CurrencyBuffer.append(ch, start, length);
	      } else if (bSTARTDATE) {
	    	  StartDateBuffer.append(ch, start, length);
	      }
	      else if (bSTOPDATE) {
	    	 	 StopDateBuffer.append(ch, start, length);
		      }
	      else if (bPRECEDENCE) {
		    	 PrecedenceBuffer.append(ch, start, length);
		      }
	      else if (bCOUNTRY) {
	    	  CountryBuffer.append(ch, start, length);
		      }
	      }


//	writeCatalogEntryOfferPriceCSV method is used for writing the processed data to the CatalogEntryOfferPrice.csv file 	   
	   private void writeCatalogEntryOfferPriceCSV()
	   {
		  output.delete(0, output.length());
		  if(hb.getCountry().toString().equals("US"))
		  {
			  Language = langId;
			  Store = store;
		  }
		  else if(hb.getCountry().toString().equals("CA"))
		  {
			  Language = langId;
			  Store = store;
		  }
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryOfferPrice.csv for language "+Language+" and Store"+Store);
		  output.append(hb.getPartnumber()+","
				  +hb.getPrice()+","+hb.getPrice()+","+hb.getCurrency()
				  +","+hb.getAnnouncedate()+","+hb.getWithdrawdate()+","+hb.getPrecedence()
				  +","+Language+",");
		  output.append(System.getProperty("line.separator"));
		  output.append(hb.getPartnumber()+suffix+","
				  +hb.getPrice()+","+hb.getPrice()+","+hb.getCurrency()
				  +","+hb.getAnnouncedate()+","+hb.getWithdrawdate()+","+hb.getPrecedence()
				  +","+Language+",");
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
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntryOfferPrice.csv \n"+e.getMessage());
			}
	   }
	
//		writeCatalogEntryOfferPriceListCSV method is used for writing the processed data to the CatalogEntryOfferPriceList.csv file 	   
	   private void writeCatalogEntryOfferPriceListCSV()
	   {
		  output1.delete(0, output1.length());
		  if(hb.getCountry().toString().equals("US"))
		  {
			  Language = langId;
			  Store = store;
		  }
		  else if(hb.getCountry().toString().equals("CA"))
		  {
			  Language = langId;
			  Store = store;
		  }
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryOfferPriceList.csv for language "+Language+" and Store"+Store);
		  output1.append(hb.getPartnumber()+","
				  +hb.getPrice()+","+hb.getPrice()+","+hb.getCurrency()
				  +",");
		  output1.append(System.getProperty("line.separator"));
		  output1.append(hb.getPartnumber()+suffix+","
				  +hb.getPrice()+","+hb.getPrice()+","+hb.getCurrency()
				  +",");
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
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntryOfferPrice.csv \n"+e.getMessage());
			}
	   }	   
	   
//	createOutputFiles method is used for creating headers to CatalogEntryOfferPrice.csv file and CatalogEntryOfferPriceList.csv file	   
	   public void createOutputFiles()
	{
		try
		{
//	Below are for creating CatalogEntryOfferPrice.csv file			
		catalogentryofferpricefw = new FileWriter(outputFile);
        out = new BufferedWriter(catalogentryofferpricefw);
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryOfferPrice for CatalogEntryOfferPrice.csv");
        output.append("CatalogEntryOfferPrice");
        out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntryOfferPrice.csv");
		output.delete(0, output.length());
		output.append("PartNumber"+","+"Price"+","+"ListPrice"+","+"CurrencyCode"+","+
				"StartDate"+","+"EndDate"+","+"Precedence"+","+"Language"+","+
				"Delete");
		out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntryOfferPrice CSV ended");
		log.info(" ");
		log.info(" ");
		
//		Below are for creating CatalogEntryOfferPriceList.csv file			
		catentofferpricelistfw = new FileWriter(outputFile1);
        out1 = new BufferedWriter(catentofferpricelistfw);
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryOfferPriceList for CatalogEntryOfferPriceList.csv");
        output1.append("CatalogEntryOfferPriceList");
        out1.write(output1.toString());
		out1.newLine();
		out1.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntryOfferPriceList.csv");
		output1.delete(0, output1.length());
		output1.append("PartNumber"+","+"Price"+","+"ListPrice"+","+"CurrencyCode"+","+
				"Delete");
		out1.write(output1.toString());
		out1.newLine();
		out1.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntryOfferPrice CSV ended");
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
		inputFile = prop.getProperty("inputFile_price");
		store = prop.getProperty("store");
		suffix = prop.getProperty("suffix");
		outputFile= prop.getProperty("outputFile_price");
		outputFile1= prop.getProperty("outputFile_pricelist");
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

