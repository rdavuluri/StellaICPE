//**************************************************************************************************************************
// The purpose of this code is to create WCS7 understandable CSV for CatalogEntryAttributeDictionaryAttributeRelationship
// Author:
// Email: 
// Version: 1.0
//**************************************************************************************************************************

package com.ibm.commerce.stella.dataload.handler;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.ibm.commerce.stella.dataload.form.HandlerBean;

public class AttributeValueHandlerIcpe extends DefaultHandler {
	
//	Below are boolean values which will be used if the SAX Parser encounters the corresponding tags
	   boolean bPARTNUMBER = false;
	   boolean bATTRIBTOKEN = false; 
	   boolean bATTRIBVAL = false;  
	   boolean bSEQUENCE = false; 
	   boolean bCOUNTRY = false;
	   boolean bLANGUAGE = false;
	   
//	Below value is used for keeping track of the number of element
	   int elementNumber=1;
	   
//	Below are log4j related	   
	  // Logger log = Logger.getLogger(this.getClass());
	   Logger log = Logger.getLogger("attributevalueLogger");	   
	   String log4JPropertyFile;
	   
//	Below are properties related 
	   String propFileName;
       String Language;
	   String inputFile;
	   String Usage;
	   String suffix;
	   String Store;
	   String store;
	   String outputFile;
	   String langId;
	   String langId_other;
	   
//	Below are used in SAX Parser for writing the values of the xml tags	   
	   public StringBuffer output = new StringBuffer();
	   public StringBuffer PartNumberBuffer;
	   public StringBuffer AttribTokenBuffer;
	   public StringBuffer AttribValBuffer;
	   public StringBuffer SequenceBuffer;
	   public StringBuffer CountryBuffer;
	   public StringBuffer LanguageBuffer;
	   BufferedWriter out;
	   FileWriter catalogentryattributedictionaryattributerelationshipfw;
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
		  } else if (qName.equalsIgnoreCase("ATTRIBTOKEN")) {
	    	  AttribTokenBuffer = new StringBuffer();
	    	  bATTRIBTOKEN = true;
	      }else if (qName.equalsIgnoreCase("ATTRIBVAL")) {
	    	  AttribValBuffer = new StringBuffer();
	    	  bATTRIBVAL = true;
	      }
	      else if (qName.equalsIgnoreCase("SEQUENCE")) {
	    	  SequenceBuffer = new StringBuffer();
	    	  bSEQUENCE = true;
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
		   }  else if (qName.equalsIgnoreCase("COUNTRY")) {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+CountryBuffer.toString().trim());
			   hb.setCountry(CountryBuffer.toString().trim());  
			   bCOUNTRY = false;
		      } else if (qName.equalsIgnoreCase("LANGUAGE")) {
				   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
				   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+LanguageBuffer.toString().trim());
				   hb.setLanguage(LanguageBuffer.toString().trim());  
			         bLANGUAGE = false;
			  } else if (qName.equalsIgnoreCase("ATTRIBTOKEN")) {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+AttribTokenBuffer.toString().trim());
			   hb.setAttribtoken(AttribTokenBuffer.toString().trim());  
			   bATTRIBTOKEN = false;
		      } else if (qName.equalsIgnoreCase("ATTRIBVAL")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+AttribValBuffer.toString().trim());
		    	  String temp = AttribValBuffer.toString().trim().replaceAll("\n","").replace(",","&#44;");
		    	  hb.setAttribval(temp);
		    	  bATTRIBVAL = false;
		      }
		      else if (qName.equalsIgnoreCase("SEQUENCE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+SequenceBuffer.toString().trim());
		    	  hb.setSequence(SequenceBuffer.toString().trim());
		    	  bSEQUENCE = false;
		      }
		   

	      if (qName.equalsIgnoreCase("ATTRIBVALUE")) {
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          End of element "+elementNumber);
	    	  log.info(" ");
	    	  log.info(" ");
	    	  writeAttributeValueCSV();
	    	  elementNumber++;
	      }
	   }

//Below method is used for reading character data of the input xml tags	   
	   //@Override
	   public void characters(char ch[], 
	      int start, int length) throws SAXException {
	      if (bPARTNUMBER) {
	    	  PartNumberBuffer.append(ch, start, length);
	      } else if (bCOUNTRY) {
	    	 CountryBuffer.append(ch, start, length);
	      } else if (bLANGUAGE) {
	    	 LanguageBuffer.append(ch, start, length);
	      } else if (bATTRIBTOKEN) {
	    	  AttribTokenBuffer.append(ch, start, length);
	      } else if (bATTRIBVAL) {
	    	  AttribValBuffer.append(ch, start, length);
	      }
	        else if (bSEQUENCE) {
	          SequenceBuffer.append(ch, start, length);
		      }	      
	      }


//	writeCatalogEntryCSV method is used for writing the processed data to the CatalogEntry.csv file 	   
	   private void writeAttributeValueCSV()
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
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryAttributeDictionaryAttributeRelationship.csv for language "+Language+" and Store"+Store);
		  output.append(hb.getPartnumber()+","+hb.getAttribtoken()+","+hb.getPartnumber()+hb.getAttribtoken()+","+Language+","+hb.getAttribval()+","+Usage+","+hb.getSequence()+",");				    
		  output.append(System.getProperty("line.separator"));
		  output.append(hb.getPartnumber()+suffix+","+hb.getAttribtoken()+","+hb.getPartnumber()+hb.getAttribtoken()+","+Language+","+hb.getAttribval()+","+Usage+","+hb.getSequence()+",");
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
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntryAttributeDictionaryAttributeRelationship.csv \n"+e.getMessage());
			}
	   }
	
	   

	   
//	createOutputFiles method is used for creating headers to CatalogEntryAttributeDictionaryAttributeRelationship.csv file
	   public void createOutputFiles()
	{
		try
		{
//	Below are for creating CatalogEntryAttributeDictionaryAttributeRelationship.csv file	
	 catalogentryattributedictionaryattributerelationshipfw = new FileWriter(outputFile);
     out = new BufferedWriter(catalogentryattributedictionaryattributerelationshipfw);
     log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Headers for CatalogEntryAttributeDictionaryAttributeRelationship.csv");
     output.append("CatalogEntryAttributeDictionaryAttributeRelationship");
     out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntryAttributeDictionaryAttributeRelationship.csv");
		output.delete(0, output.length());
		output.append("PartNumber"+","+"AttributeIdentifier"+","+"ValueIdentifier"+","+"LanguageId"+","+"Value"+","+"Usage"+","+"Sequence"+","+"Delete");
	    out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntryAttributeDictionaryAttributeRelationship CSV ended");
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
		langId_other = prop.getProperty("langId_other");
		suffix = prop.getProperty("suffix");
		inputFile = prop.getProperty("inputFile_attribvalue");
		store = prop.getProperty("store");
		outputFile= prop.getProperty("outputFile_attribvalue");
		Usage = prop.getProperty("usage");
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
