//**************************************************************************************************************************
// The purpose of this code is to create WCS7 understandable CSV for CatalogEntry and CatalogEntryParentProductRelationship
// Author: Vignesh Venkatraman
// Email: vigneven@in.ibm.com
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
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import com.ibm.commerce.stella.dataload.form.*;

public class ProductHandlerIcpe extends DefaultHandler {

//	Below are boolean values which will be used if the SAX Parser encounters the corresponding tags
	   boolean bPARTNUMBER = false;
	   boolean bCOUNTRY = false;
	   boolean bLANGUAGE = false;
	   boolean bAUXDESC1 = false;
	   boolean bAUXDESC2 = false;
	   boolean bSHORTDESC = false;
	   boolean bANNOUNCEDATE = false;
	   boolean bWITHDRAWDATE = false;
	   boolean bPUBLISHED = false;
	   boolean bIMAGE = false;
	   boolean bMARKETDESC = false;
	   boolean status = false;
//	Below value is used for keeping track of the number of element
	   int elementNumber=1;
	   
//	Below are log4j related	   
	  // Logger log = Logger.getLogger(this.getClass());
	   Logger log = Logger.getLogger("productLogger");	   
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
	   String langId_other;
	   String ItemBean;
	   String ProductBean;
	   String Available;
	   String Buyable;
	   String ImageDir;
	   
	   
//	Below are used in SAX Parser for writing the values of the xml tags	   
	   public StringBuffer output = new StringBuffer();
	   public StringBuffer output1 = new StringBuffer();
	   public StringBuffer Auxdesc1Buffer;
	   public StringBuffer Auxdesc2Buffer; 
	   public StringBuffer PartNumberBuffer;
	   public StringBuffer CountryBuffer;
	   public StringBuffer LanguageBuffer;
	   public StringBuffer ShortDescBuffer;
	   public StringBuffer AnnounceDateBuffer;
	   public StringBuffer WithDrawDateBuffer;
	   public StringBuffer PublishedBuffer;
	   public StringBuffer ImageBuffer;
	   public StringBuffer MarketDescBuffer;
	   BufferedWriter out;
	   BufferedWriter out1;
	   FileWriter catalogentryfw;
	   FileWriter catentrelfw;
	   
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
	      } else if (qName.equalsIgnoreCase("COUNTRY")) {
	    	  CountryBuffer = new StringBuffer();
	         bCOUNTRY = true;
	      } else if (qName.equalsIgnoreCase("LANGUAGE")) {
	    	  LanguageBuffer = new StringBuffer();
		     bLANGUAGE = true;
		  }else if (qName.equalsIgnoreCase("AUXDESC1")) {
	    	  Auxdesc1Buffer = new StringBuffer();
	          bAUXDESC1 = true;
	      }
	      else if (qName.equalsIgnoreCase("AUXDESC2")) {
	    	  Auxdesc2Buffer = new StringBuffer();
	         bAUXDESC2 = true;
	      }
	      else if (qName.equalsIgnoreCase("SHORTDESC")) {
	    	  ShortDescBuffer = new StringBuffer();
	    		  bSHORTDESC = true;
	    		  }
	      else if (qName.equalsIgnoreCase("ANNOUNCEDATE")) {
	    	  AnnounceDateBuffer = new StringBuffer();
    		  bANNOUNCEDATE = true;
	   }
	      else if (qName.equalsIgnoreCase("WITHDRAWDATE")) {
	    	  WithDrawDateBuffer = new StringBuffer();
    		  bWITHDRAWDATE = true;
	   }
	      else if (qName.equalsIgnoreCase("PUBLISHED")) {
	    	  PublishedBuffer = new StringBuffer();
    		  bPUBLISHED = true;
	   }
	      else if (qName.equalsIgnoreCase("IMAGE")) {
	    	  ImageBuffer = new StringBuffer();
    		  bIMAGE = true;
	   }
	      else if (qName.equalsIgnoreCase("MARKETDESC")) {
	    	  MarketDescBuffer = new StringBuffer();
    		  bMARKETDESC = true;
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
		   else if (qName.equalsIgnoreCase("COUNTRY")) {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+CountryBuffer.toString().trim());
			   hb.setCountry(CountryBuffer.toString().trim());  
		         bCOUNTRY = false;
		      } else if (qName.equalsIgnoreCase("LANGUAGE")) {
				   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
				   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+LanguageBuffer.toString().trim());
				   hb.setLanguage(LanguageBuffer.toString().trim());  
			         bLANGUAGE = false;
			  } else if (qName.equalsIgnoreCase("AUXDESC1")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+Auxdesc1Buffer.toString().trim());
		    	  String temp = Auxdesc1Buffer.toString().trim().replaceAll("\n","").replace(",","&#44;");
		    	  hb.setAuxdesc1(temp);
		         bAUXDESC1 = false;
		      }
		      else if (qName.equalsIgnoreCase("AUXDESC2")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+Auxdesc2Buffer.toString().trim());
		    	  String temp = Auxdesc2Buffer.toString().trim().replaceAll("\n","").replace(",","&#44;");
		    	  hb.setAuxdesc2(temp);
		         bAUXDESC2 = false;
		      }
		      else if (qName.equalsIgnoreCase("SHORTDESC")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+ShortDescBuffer.toString().trim());
		    	  String temp = ShortDescBuffer.toString().trim().replaceAll("\n","").replace(",","&#44;");
		    	  hb.setShortdesc(temp);
		    		  bSHORTDESC = false;
		    		  }
		      else if (qName.equalsIgnoreCase("ANNOUNCEDATE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+AnnounceDateBuffer.toString().trim());
//	The date received from upstream is converted to WCS7 understandable format below		    	  
		    	  AnnounceDateBuffer.append(" "+Datesuffix);
		    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Added date suffix "+Datesuffix);
		    	  hb.setAnnouncedate(AnnounceDateBuffer.toString().trim());
	    		  bANNOUNCEDATE = false;
		   }
		      else if (qName.equalsIgnoreCase("WITHDRAWDATE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+WithDrawDateBuffer.toString().trim());
		    	  WithDrawDateBuffer.append(" "+Datesuffix);
		    	  hb.setWithdrawdate(WithDrawDateBuffer.toString().trim());
		    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Added date suffix "+Datesuffix);
	    		  bWITHDRAWDATE = false;
		   }
		      else if (qName.equalsIgnoreCase("PUBLISHED")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+PublishedBuffer.toString().trim());
		    	  hb.setPublished(PublishedBuffer.toString().trim());
	    		  bPUBLISHED = false;
		   }
		      else if (qName.equalsIgnoreCase("IMAGE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+ImageBuffer.toString().trim());
		    	  hb.setImage(ImageBuffer.toString().trim());
	    		  bIMAGE = false;
		   }
		      else if (qName.equalsIgnoreCase("MARKETDESC")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Creating WCS7 compatible value for marketdesc");
//	Below is used for 2 purposes
//	1) To replace comma symbol with ascii value since in CSV it will consider as a separate field	
//	2) To replace new line with a blank space		    	  
		    	  String temp = MarketDescBuffer.toString().trim().replaceAll("\n","").replace(",","&#44;");
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+temp);
		    	  hb.setMarketdesc(temp);
	    		  bMARKETDESC = false;
		   }

	      if (qName.equalsIgnoreCase("SINGLEDESC")) {
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          End of element "+elementNumber);
	    	  log.info(" ");
	    	  log.info(" ");
	    	  writeCatalogEntryCSV();
	    	  writeCatalogEntryParentProductRelationshipCSV();
	    	  elementNumber++;
	      }
	   }

// Below method is used for reading character data of the input xml tags	   
	   //@Override
	   public void characters(char ch[], 
	      int start, int length) throws SAXException {
	      if (bPARTNUMBER) {
	    	  PartNumberBuffer.append(ch, start, length);
	      } else if (bCOUNTRY) {
	    	 CountryBuffer.append(ch, start, length);
	      } else if (bLANGUAGE) {
	    	 LanguageBuffer.append(ch, start, length);
	      } else if (bAUXDESC1) {
	    	  Auxdesc1Buffer.append(ch, start, length);
	      } else if (bAUXDESC2) {
	    	  Auxdesc2Buffer.append(ch, start, length);
	      }
	      else if (bSHORTDESC) {
	    	 	 ShortDescBuffer.append(ch, start, length);
		      }
	      else if (bANNOUNCEDATE) {
		    	 AnnounceDateBuffer.append(ch, start, length);
		      }
	      else if (bWITHDRAWDATE) {
	    	  WithDrawDateBuffer.append(ch, start, length);
		      }
	      else if (bPUBLISHED) {
	    	  PublishedBuffer.append(ch, start, length);
		      }
	      else if (bIMAGE) {
	    	  ImageBuffer.append(ch, start, length);
		      }
	      else if (bMARKETDESC) {
	    	  MarketDescBuffer.append(ch, start, length);
		      }
	      }


//	writeCatalogEntryCSV method is used for writing the processed data to the CatalogEntry.csv file 	   
	   private void writeCatalogEntryCSV()
	   {
		  output.delete(0, output.length());
		  if(hb.getLanguage().toString().equals("en"))
		  {
			  Language = langId;
			  Store = store;
		  }
		  else if(hb.getLanguage().toString().equals("fr"))
		  {
			  Language = langId_other;
			  Store = store;
		  }
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntry.csv for language "+Language+" and Store"+Store);
		  
		  output.append(hb.getPartnumber()+","+ProductBean+","+Store+","+Language+","
				  +hb.getAuxdesc1()+","+hb.getAuxdesc2()+","+hb.getShortdesc()
				  +","+hb.getShortdesc()+","+hb.getAnnouncedate()+","+hb.getWithdrawdate()
				  +","+hb.getWithdrawdate()+","+hb.getPublished()+","+hb.getImage()+","+hb.getImage()+","+hb.getMarketdesc()+","+Available+","+Buyable+",");
		  output.append(System.getProperty("line.separator"));
		  output.append(hb.getPartnumber()+suffix+","+ItemBean+","+Store+","+Language+","
				  +hb.getAuxdesc1()+","+hb.getAuxdesc2()+","+hb.getShortdesc()
				  +","+hb.getShortdesc()+","+hb.getAnnouncedate()+","+hb.getWithdrawdate()
				  +","+hb.getWithdrawdate()+","+hb.getPublished()+","+hb.getImage()+","+hb.getImage()+","+hb.getMarketdesc()+","+Available+","+Buyable+",");
		  
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
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntry.csv \n"+e.getMessage());
			}
	   }
	
	   
//	writeCatalogEntryParentProductRelationshipCSV method is used for writing the processed data to the CatalogEntryParentProductRelationship.csv file	   
public void writeCatalogEntryParentProductRelationshipCSV()
{
	output1.delete(0, output.length());
	if(hb.getLanguage().toString().equals("en"))
	  {
		  Language = langId;
		  Store = store;
	  }
	  else if(hb.getLanguage().toString().equals("fr"))
	  {
		  Language = langId_other;
		  Store = store;
	  }
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryParentProductRelationship.csv for language "+Language+" and Store"+Store);
		  output1.append(hb.getPartnumber()+suffix+","+hb.getPartnumber()+",");
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
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntryParentProductRelationship.csv \n"+e.getMessage());
			}
	   }


	   
//	createOutputFiles method is used for creating headers to CatalogEntry.csv file and CatalogEntryParentProductRelationship.csv file	   
	   public void createOutputFiles()
	{
		try
		{
//	Below are for creating CatalogEntry.csv file	
		catalogentryfw = new FileWriter(outputFile);
        out = new BufferedWriter(catalogentryfw);
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntry for CatalogEntry.csv");
        output.append("CatalogEntry");
        out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntry.csv");
		output.delete(0, output.length());
		output.append("PartNumber"+","+"Type"+","+"ParentStoreIdentifier"+","+"LanguageId"+","+
				"AuxDescription1"+","+"AuxDescription2"+","+"Name"+","+"ShortDescription"+","+
				"StartDate"+","+"EndDate"+","+"AvailabilityDate"+","+"Published"+","+"Thumbnail"+","+"FullImage"+
				","+"LongDescription"+","+"Available"+","+"Buyable"+","+"Delete");
		out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntry CSV ended");
		log.info(" ");
		log.info(" ");
		
		
//	Below are for writing CatalogEntryParentProductRelationship.csv file	
		catentrelfw = new FileWriter(outputFile1);
		out1 = new BufferedWriter(catentrelfw);
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryParentProductRelationship for CatalogEntryParentProductRelationship.csv");
		output1.append("CatalogEntryParentProductRelationship");
		out1.write(output1.toString());
		out1.newLine();
		out1.flush();
		output1.delete(0, output1.length());
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntryParentProductRelationship.csv");
		output1.append("PartNumber"+","+"ParentPartNumber"+","+"Delete");
		out1.write(output1.toString());
		out1.newLine();
		out1.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntryParentProductRelationship CSV ended");
		log.info(" ");
		log.info(" ");
		
		}
		catch(Exception e)
		{
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught in createOutputFilesMethod \n"+e.getMessage());
		}
		/* */
		status = parse();
		if (!status) {
			log.info(new Timestamp(date.getTime())+"		INFORMATION:	Parser failed");
			System.exit(1);
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
		langId_other = prop.getProperty("langId_other");
		inputFile = prop.getProperty("inputFile_product");
		store = prop.getProperty("store");
		suffix = prop.getProperty("suffix");
		outputFile= prop.getProperty("outputFile_product");
		outputFile1 = prop.getProperty("outputFile_parentprod");
		ItemBean = prop.getProperty("item");
		Datesuffix = prop.getProperty("datesuffix");
		ProductBean = prop.getProperty("product");
		Available = prop.getProperty("available");
		Buyable = prop.getProperty("buyable");
		ImageDir = prop.getProperty("imageDir");
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


private boolean parse() {
	try {
		XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
		parser.setContentHandler(this);
		parser.setErrorHandler(this);
		parser.parse(inputFile);		
	}
	catch(Exception e)
	{
		 log.error(new Timestamp(date.getTime())+"		ERROR:		Invalid xml file. "+e.getMessage());
		 return false;
	}
	return true;
}

}

