//**************************************************************************************************************************
// The purpose of this code is to create WCS7 understandable CSV for AttributeDictionaryAttributeAndAllowedValues
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

public class AttributeHandlerIcpe extends DefaultHandler {
//	Below are boolean values which will be used if the SAX Parser encounters the corresponding tags
	   boolean bATTRIBTOKEN = false;
	   boolean bATTRIBTYPE = false;
	   boolean bUSAGE = false;
	   boolean bSEQUENCE = false;
	   boolean bATTRIBNAME = false;
	   boolean bLANGUAGE = false;
	   

	   
//	Below value is used for keeping track of the number of element
	   int elementNumber=1;
	   
//	Below are log4j related	   
	   //Logger log = Logger.getLogger(this.getClass());
	   Logger log = Logger.getLogger("attributeLogger");
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
	   String langId;
	   String langId_other;
	   String ItemBean;
	   String ProductBean;
	   String DisplayableAttr;
	   String ComparableAttr;
	   String FacetableAttr;
	   
//	Below are used in SAX Parser for writing the values of the xml tags	   
	   public StringBuffer output = new StringBuffer();
	   public StringBuffer AttribTokenBuffer;
	   public StringBuffer SequenceBuffer; 
	   public StringBuffer AttribTypeBuffer;
	   public StringBuffer UsageBuffer;
	   public StringBuffer AttribNameBuffer;
	   public StringBuffer LanguageBuffer;
	   BufferedWriter out;
	   FileWriter attrDictAttrAndAllowedValfw;
	   
	   
//	Below is for creating object for HandlerBean where all the values of the xml tags will be set	   
	   HandlerBean hb = new HandlerBean();
	   Date date = new Date();
	   
	   
	   
//	The below method startElement is the first method invoked by SAX Parser when we call parse method    
	   //@Override
	   public void startElement(String uri, 
	   String localName, String qName, Attributes attributes)
	      throws SAXException {
		   
	      if (qName.equalsIgnoreCase("ATTRIBTOKEN")) {
	    	  AttribTokenBuffer = new StringBuffer();
	    	  bATTRIBTOKEN = true;
	      } else if (qName.equalsIgnoreCase("ATTRIBTYPE")) {
	    	  AttribTypeBuffer = new StringBuffer();
	         bATTRIBTYPE = true;
	      }else if (qName.equalsIgnoreCase("USAGE")) {
	    	  UsageBuffer = new StringBuffer();
	         bUSAGE = true;
	      }else if (qName.equalsIgnoreCase("SEQUENCE")) {
	    	  SequenceBuffer = new StringBuffer();
	         bSEQUENCE = true;
	      } else if (qName.equalsIgnoreCase("ATTRIBNAME")) {
	    	  AttribNameBuffer = new StringBuffer();
	         bATTRIBNAME = true;
	      } else if (qName.equalsIgnoreCase("LANGUAGE")) {
	    	  LanguageBuffer = new StringBuffer();
		     bLANGUAGE = true;
		  }
	      
	   }

	   
//	Below is where the actual processing of xml elements starts and the character data is fetched and written to appropriate CSVs 
	   //@Override
	   public void endElement(String uri, 
	   String localName, String qName) throws SAXException {
		   if(qName.equalsIgnoreCase("ATTRIBTOKEN"))
		   {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+AttribTokenBuffer.toString().trim());
//	The value is set using setter 			   
			   hb.setAttribtoken(AttribTokenBuffer.toString().trim());
		     bATTRIBTOKEN = false;
		   }
		   else if (qName.equalsIgnoreCase("ATTRIBTYPE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+AttribTypeBuffer.toString().trim());
		    	  hb.setAttribtype(AttribTypeBuffer.toString().trim());
		         bATTRIBTYPE = false;
		      } else if (qName.equalsIgnoreCase("USAGE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+UsageBuffer.toString().trim());
		    	  hb.setUsage(UsageBuffer.toString().trim());
	    		  bUSAGE = false;
		   }
		   else if (qName.equalsIgnoreCase("SEQUENCE")) {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+SequenceBuffer.toString().trim());
			   hb.setSequence(SequenceBuffer.toString().trim());  
		         bSEQUENCE = false;
		      } else if (qName.equalsIgnoreCase("ATTRIBNAME")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+AttribNameBuffer.toString().trim());
		    	  hb.setAttribname(AttribNameBuffer.toString().trim());
		         bATTRIBNAME = false;
		      } else if (qName.equalsIgnoreCase("LANGUAGE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+LanguageBuffer.toString().trim());
		    	  hb.setLanguage(LanguageBuffer.toString().trim());
		         bLANGUAGE = false;
		      }
		      
		     

	      if (qName.equalsIgnoreCase("ATTRIBUTE")) {
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          End of element "+elementNumber);
	    	  log.info(" ");
	    	  log.info(" ");
	    	  writeAttributeDictionaryAttributeAndAllowedValuesCSV();
	    	  elementNumber++;
	      }
	   }

//Below method is used for reading character data of the input xml tags	   
	   //@Override
	   public void characters(char ch[], 
	      int start, int length) throws SAXException {
	      if (bATTRIBTOKEN) {
	    	  AttribTokenBuffer.append(ch, start, length);
	      }else if (bATTRIBTYPE) {
	    	  AttribTypeBuffer.append(ch, start, length);
	      }else if (bUSAGE) {
	    	  UsageBuffer.append(ch, start, length);
	      } else if (bSEQUENCE) {
	    	 SequenceBuffer.append(ch, start, length);
	      } else if (bATTRIBNAME) {
	    	  AttribNameBuffer.append(ch, start, length);
	      } else if (bLANGUAGE) {
	    	  LanguageBuffer.append(ch, start, length);
	      } 
	      
	      }


//	writeAttributeDictionaryAttributeAndAllowedValuesCSV method is used for writing the processed data to the AttributeDictionaryAttributeAndAllowedValues.csv file 	   
	   private void writeAttributeDictionaryAttributeAndAllowedValuesCSV()
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
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for AttributeDictionaryAttributeAndAllowedValues.csv for language "+Language);
		  output.append(hb.getAttribtoken()+","
				  +hb.getAttribtype()+","+hb.getUsage()+","+hb.getSequence()+","+DisplayableAttr+",,"+ComparableAttr+","+FacetableAttr+","+Language+","+hb.getAttribname()+",,,,,,");
		  //output.append(System.getProperty("line.separator"));
		  /*output.append(hb.getPartnumber()+suffix+","
				  +hb.getIdentifier()+","+Store+","+hb.getSequence()
				  +","); */
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
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing AttributeDictionaryAttributeAndAllowedValues.csv \n"+e.getMessage());
			}
	   }
	
	   
	   
//	createOutputFiles method is used for creating headers to AttributeDictionaryAttributeAndAllowedValues.csv file	   
	   public void createOutputFiles()
	{
		try
		{
//	Below are for creating AttributeDictionaryAttributeAndAllowedValues.csv file			
		attrDictAttrAndAllowedValfw = new FileWriter(outputFile);
     out = new BufferedWriter(attrDictAttrAndAllowedValfw);
     log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header AttributeDictionaryAttributeAndAllowedValues for AttributeDictionaryAttributeAndAllowedValues.csv");
     output.append("AttributeDictionaryAttributeAndAllowedValues");
     out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for AttributeDictionaryAttributeAndAllowedValues.csv");
		output.delete(0, output.length());
		output.append("Identifier"+","+"Type"+","+"AttributeType"+","+"Sequence"+","+"Displayable"+","+"StoreDisplay"+","+"Comparable"+","+"Facetable"+","+"LanguageId"+","+"Name"+","+"AllowedValue1"+","+"AllowedValue2"+","+"AllowedValue3"+","+"AllowedValue4"+","+"AllowedValue5"+","+"Delete");
		out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in AttributeDictionaryAttributeAndAllowedValues CSV ended");
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
		inputFile = prop.getProperty("inputFile_attribute");
		store = prop.getProperty("store");
		suffix = prop.getProperty("suffix");
		outputFile= prop.getProperty("outputFile_attribute");
		ItemBean = prop.getProperty("item");
		Datesuffix = prop.getProperty("datesuffix");
		ProductBean = prop.getProperty("product");
		DisplayableAttr = prop.getProperty("displayable");
		ComparableAttr = prop.getProperty("comparable");
		FacetableAttr = prop.getProperty("facetable");
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
