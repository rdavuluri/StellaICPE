//**************************************************************************************************************************
// The purpose of this code is to create WCS7 understandable CSV for CatalogEntryParentCatalogGroupRelationship
// Author:
// Email: 
// Version: 1.0
//**************************************************************************************************************************

package com.ibm.commerce.stella.dataload.handler;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import com.ibm.commerce.stella.dataload.form.*;

public class CatProdHandlerIcpe extends DefaultHandler {

//	Below are boolean values which will be used if the SAX Parser encounters the corresponding tags
	   boolean bPARTNUMBER = false;
	   boolean bSEQUENCE = false;
	   boolean bIDENTIFIER = false;
	   boolean bCOUNTRY = false;
	   boolean bLANGUAGE = false;
// Below is the 	   
	   private static String sabrixcodecategory1=null;
	   private static String sabrixcodecategory2=null;
	   
	   
//	Below value is used for keeping track of the number of element
	   int elementNumber=1;
	   
	   
	   List<String> BusinessExclusionList = new ArrayList<String>();
	   List<String> BeneplaceExclusionList = new ArrayList<String>();
	   List<String> ExclusionList;
	   	
//	Below are log4j related	   
	   //Logger log = Logger.getLogger(this.getClass());
	   Logger log = Logger.getLogger("catprodLogger");
	   String log4JPropertyFile;
	   
//	Below are properties related 
	   String countryLoad;
	   String propFileName;
	   String Language;
	   String inputFile;
	   String Datesuffix;
	   String Store;
	   String desc;
	   String store;
	   String suffix;
	   String outputFile;
	   String outputFileProdCode;
	   String outputFileSales;
	   String outputFileSalesDelete;
	   String langId;
	   String langId_other;
	   String ItemBean;
	   String ProductBean;
	   String sabrixproductcode1;
	   String sabrixproductcode2;
	   String sabprodcode;
	   String productExclusionFile;
	   String outputFileBusiness;
	   String outputFileBusinessDelete;
	   String outputFileBeneplace;
	   String outputFileBeneplaceDelete;
	   String productNumber;
	   String productStore;
	   String storeSuffix;
	   String Country_Suffix;
	   String Business_Suffix;
	   String Beneplace_Suffix;
	   String outputFileCountry;
	   
//	Below are used in SAX Parser for writing the values of the xml tags	   
	   public StringBuffer output = new StringBuffer();
	   public StringBuffer outputProdCode = new StringBuffer();
	   public StringBuffer outputCountry = new StringBuffer();
	   public StringBuffer outputSales = new StringBuffer();
	   public StringBuffer outputSalesDelete = new StringBuffer();
	   public StringBuffer PartNumberBuffer;
	   public StringBuffer SequenceBuffer; 
	   public StringBuffer IdentifierBuffer;
	   public StringBuffer CountryBuffer;
	   public StringBuffer LanguageBuffer;
	   BufferedWriter out;
	   BufferedWriter outProdCode;
	   BufferedWriter outCountry;
	   BufferedWriter outSales;
	   BufferedWriter outSalesDelete;
	   FileWriter catalogentryparentcatalogfw;
	   FileWriter catalogentryproductcodefw;
	   FileWriter catentparentcatalogcountryfw;
	   FileWriter catentparentcatalogsalesfw;
	   FileWriter catentparentcatalogsalesdeletefw;
	   FileReader in;
	   BufferedReader br;
	   
	   
	   LinkedHashMap<String, List<String>> productDetails = new LinkedHashMap<String, List<String>>();
	   
//	Below is for creating object for HandlerBean where all the values of the xml tags will be set	   
	   HandlerBean hb = new HandlerBean();
	   Date date = new Date();
	   String[] sabcodecat = null;
//	   sabcodecat = sabrixcodecategory.split(",");
	   
	   
//	The below method startElement is the first method invoked by SAX Parser when we call parse method    
	   //@Override
	   public void startElement(String uri, 
	   String localName, String qName, Attributes attributes)
	      throws SAXException {
		   
	      if (qName.equalsIgnoreCase("PARTNUMBER")) {
	    	  PartNumberBuffer = new StringBuffer();
	    	  bPARTNUMBER = true;
	      } else if (qName.equalsIgnoreCase("SEQUENCE")) {
	    	  SequenceBuffer = new StringBuffer();
	         bSEQUENCE = true;
	      } else if (qName.equalsIgnoreCase("IDENTIFIER")) {
	    	  IdentifierBuffer = new StringBuffer();
	         bIDENTIFIER = true;
	      }
	      else if (qName.equalsIgnoreCase("COUNTRY")) {
	    	  CountryBuffer = new StringBuffer();
	         bCOUNTRY = true;
	      }
	      else if (qName.equalsIgnoreCase("LANGUAGE")) {
	    	  LanguageBuffer = new StringBuffer();
	         bLANGUAGE = true;
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
		   else if (qName.equalsIgnoreCase("SEQUENCE")) {
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
			   log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+SequenceBuffer.toString().trim());
			   hb.setSequence(SequenceBuffer.toString().trim());  
		         bSEQUENCE = false;
		      } else if (qName.equalsIgnoreCase("IDENTIFIER")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+IdentifierBuffer.toString().trim());
		    	  hb.setIdentifier(IdentifierBuffer.toString().trim());
		         bIDENTIFIER = false;
		      } else if (qName.equalsIgnoreCase("COUNTRY")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+CountryBuffer.toString().trim());
		    	  hb.setCountry(CountryBuffer.toString().trim());
	    		  bCOUNTRY = false;
		      } else if (qName.equalsIgnoreCase("LANGUAGE")) {
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
		    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Value "+LanguageBuffer.toString().trim());
		    	  hb.setLanguage(LanguageBuffer.toString().trim());
	    		  bLANGUAGE = false;
		   }
		      
		     

	      if (qName.equalsIgnoreCase("CATGPENREL")) {
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          Element "+qName);
	    	  log.debug(new Timestamp(date.getTime())+"		Debug:          End of element "+elementNumber);
	    	  log.info(" ");
	    	  log.info(" ");
	    	  
	    	  //Saving product details in Map for checking product exclusion list and creating sales catprod files
	    	  List<String> productvalue = new ArrayList<String>();
    		  productvalue.add(hb.getSequence());
    		  productvalue.add(hb.getIdentifier());
    		  productvalue.add(hb.getCountry());
    		  productDetails.put(hb.getPartnumber(),productvalue);
	  		  
	    	  
	    	  writeCatalogEntryParentCatalogGroupRelationshipCSV();
	    	  writeCatalogEntryProductCodeCSV();
	    	  
	    	  elementNumber++;
	      }
	      
	      if (qName.equalsIgnoreCase("NumberOfElements"))
	      {
	    	  setProductExlusionList();
	    	  createOutputFilesSales(outputFileBusiness,outputFileBusinessDelete);
	    	  writeCatEntParentCatalogGroupRelationshipCSV(BusinessExclusionList,Business_Suffix);
	    	  if (countryLoad.equals("US"))
	  		  {
	    		  createOutputFilesSales(outputFileBeneplace,outputFileBeneplaceDelete);
	    		  writeCatEntParentCatalogGroupRelationshipCSV(BeneplaceExclusionList,Beneplace_Suffix);
	  		  }
	      }
	   }


	// Below method is used for reading character data of the input xml tags	   
	   //@Override
	   public void characters(char ch[], 
	      int start, int length) throws SAXException {
	      if (bPARTNUMBER) {
	    	  PartNumberBuffer.append(ch, start, length);
	      } else if (bSEQUENCE) {
	    	 SequenceBuffer.append(ch, start, length);
	      } else if (bIDENTIFIER) {
	    	  IdentifierBuffer.append(ch, start, length);
	      } else if (bCOUNTRY) {
	    	  CountryBuffer.append(ch, start, length);
	      } else if (bLANGUAGE) {
	    	  LanguageBuffer.append(ch, start, length);
	      }
	      
	      }


//	writeCatalogEntryParentCatalogGroupRelationshipCSV method is used for writing the processed data to the CatalogEntryParentCatalogGroupRelationship.csv and CatEntParentCatalogGroupRelationship_US/CA.csv file 	   
	   private void writeCatalogEntryParentCatalogGroupRelationshipCSV()
	   {
		  output.delete(0, output.length());
		  outputCountry.delete(0, outputCountry.length());
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
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryParentCatalogGroupRelationship.csv for language "+Language+" and Store "+Store);
		  output.append(hb.getPartnumber()+","+hb.getIdentifier()+","+Store+","+hb.getSequence()+",");
		  output.append(System.getProperty("line.separator"));
		  output.append(hb.getPartnumber()+suffix+","+hb.getIdentifier()+","+Store+","+hb.getSequence()+",");
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Data writing completed for element"+elementNumber);
		  log.info(" ");
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatEntParentCatalogGroupRelationship"+Country_Suffix+".csv for language "+Language+" and Store "+Store);
		  outputCountry.append(hb.getPartnumber()+","+hb.getIdentifier()+Country_Suffix+","+Store+","+hb.getSequence()+",");
		  outputCountry.append(System.getProperty("line.separator"));
		  outputCountry.append(hb.getPartnumber()+suffix+","+hb.getIdentifier()+Country_Suffix+","+Store+","+hb.getSequence()+",");
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Data writing completed for element"+elementNumber);
		  log.info(" ");
		  log.info(" ");
		  try
			{
			out.write(output.toString());
			outCountry.write(outputCountry.toString());
			out.newLine();
			outCountry.newLine();
			out.flush();
			outCountry.flush();
			}
			catch(Exception e)
			{
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntryParentCatalogGroupRelationship.csv and CatEntParentCatalogGroupRelationship"+Country_Suffix+".csv \n"+e.getMessage());
			}
	   }

//		writeCatalogEntryProductCodeCSV method is used for writing the processed data to the CatalogEntryProductCode.csv file	   
	   
	   private void writeCatalogEntryProductCodeCSV()
	   {
			outputProdCode.delete(0,outputProdCode.length());

//     Below code will write the data in CatalogEntryProductCode.csv, for all the products that has sabrix product code as 23.			

			if(hb.getIdentifier().toString().equals(sabrixcodecategory2))
			{
			  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryProductCode.csv  ");
			  outputProdCode.append(hb.getPartnumber()+","+sabrixproductcode2+",");
			  outputProdCode.append(System.getProperty("line.separator"));
			  outputProdCode.append(hb.getPartnumber()+suffix+","+sabrixproductcode2+",");
			  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Data writing completed for element"+elementNumber);
			  log.info(" ");
			  log.info(" ");			  
			}	
//    Below code will write the data in CatalogEntryProductCode.csv, for all the products that has sabrix product code as 01. 			
			else if(Arrays.asList(sabcodecat).contains(hb.getIdentifier()))
			{
			  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryProductCode.csv  ");
			  outputProdCode.append(hb.getPartnumber()+","+sabrixproductcode1+",");
			  outputProdCode.append(System.getProperty("line.separator"));
			  outputProdCode.append(hb.getPartnumber()+suffix+","+sabrixproductcode1+",");
			  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Data writing completed for element"+elementNumber);
			  log.info(" ");
			  log.info(" ");				  	
			}
			else
			{
			  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing "+elementNumber+" element for CatalogEntryProductCode.csv  ");
			  outputProdCode.append(hb.getPartnumber()+","+",");
			  outputProdCode.append(System.getProperty("line.separator"));
			  outputProdCode.append(hb.getPartnumber()+suffix+","+",");
			  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Data writing completed for element"+elementNumber);
			  log.info(" ");
			  log.info(" ");				  	
			}
			try
			{
			outProdCode.write(outputProdCode.toString());
			outProdCode.newLine();
			outProdCode.flush();
			}
			catch(Exception e)
			{
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatalogEntryProductCode.csv \n"+e.getMessage());
			}
			
		}

//		writeCatEntParentCatalogGroupRelationshipCSV method is used for writing the processed data to the CatEntParentCatalogGroupRelationshipBeneplace.csv and CatEntParentCatalogGroupRelationshipDeleteBeneplace.csv file 	   
	   @SuppressWarnings("rawtypes")
	private void writeCatEntParentCatalogGroupRelationshipCSV(List<String> ExclusionList1,String storeSuffix1)
	   {
		   ExclusionList=ExclusionList1;
		   storeSuffix=storeSuffix1;
		 try
		 {
		  		  
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Writing data for "+outputFileSales+" and "+outputFileSalesDelete+"");
		  		  
		  Iterator entries = productDetails.entrySet().iterator();
		  
		  while (entries.hasNext()) {
		  outputSales.delete(0, outputSales.length());
		  outputSalesDelete.delete(0, outputSalesDelete.length());
		  
		  Map.Entry entry = (Entry) entries.next();
		  String key = (String) entry.getKey();
		  List productValues = productDetails.get(key);
		  
		  log.info(new Timestamp(date.getTime())+"		DEBUG:		Writing data for product : "+key);
		  outputSales.append(key+","+productValues.get(1)+storeSuffix+","+store+","+productValues.get(0)+",");
		  outputSales.append(System.getProperty("line.separator"));
		  outputSales.append(key+suffix+","+productValues.get(1)+storeSuffix+","+store+","+productValues.get(0)+",");
		  
		  if(ExclusionList.contains(key))
		   {
			  log.info(new Timestamp(date.getTime())+"		DEBUG:		Writing excluding beneplace data for product : "+key);
			  outputSalesDelete.append(key+","+productValues.get(1)+storeSuffix+","+store+","+productValues.get(0)+","+1);
			  outputSalesDelete.append(System.getProperty("line.separator"));
			  outputSalesDelete.append(key+suffix+","+productValues.get(1)+storeSuffix+","+store+","+productValues.get(0)+","+1);
			  try
				{
				outSalesDelete.write(outputSalesDelete.toString());
				outSalesDelete.newLine();
				outSalesDelete.flush();
				}
				catch(Exception e)
				{
					log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatEntParentCatalogGroupRelationshipDelete_Beneplace.csv \n"+e.getMessage());
				}
		   }
		  try
			{
			outSales.write(outputSales.toString());
			outSales.newLine();
			outSales.flush();
			}
			catch(Exception e)
			{
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while writing CatEntParentCatalogGroupRelationship_Beneplace.csv \n"+e.getMessage());
			}
			
		   }
	    }	
			
		catch (Exception e) {
				log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while processing input file "+e.getMessage());
				e.printStackTrace();
		}
	   }
	   
//	createOutputFiles method is used for creating headers to CatalogEntryParentCatalogGroupRelationship.csv file	   
	   public void createOutputFiles()
	{
		try
		{
//	Below are for creating CatalogEntryParentCatalogGroupRelationship.csv file			
		catalogentryparentcatalogfw = new FileWriter(outputFile);
        out = new BufferedWriter(catalogentryparentcatalogfw);
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryParentCatalogGroupRelationship for CatalogEntryParentCatalogGroupRelationship.csv");
        output.append("CatalogEntryParentCatalogGroupRelationship");
        out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntryParentCatalogGroupRelationship.csv");
		output.delete(0, output.length());
		output.append("PartNumber"+","+"ParentGroupIdentifier"+","+"ParentStoreIdentifier"+","+"Sequence"+","+"Delete");
		out.write(output.toString());
		out.newLine();
		out.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntryParentCatalogGroupRelationship CSV ended");
		log.info(" ");
		log.info(" ");

//	Below are for creating CatEntParentCatalogGroupRelationship_US/CA.csv file			
		catentparentcatalogcountryfw = new FileWriter(outputFileCountry);
        outCountry = new BufferedWriter(catentparentcatalogcountryfw);
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryParentCatalogGroupRelationship for CatEntParentCatalogGroupRelationship"+Country_Suffix+".csv");
        outputCountry.append("CatalogEntryParentCatalogGroupRelationship");
        outCountry.write(outputCountry.toString());
		outCountry.newLine();
		outCountry.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatEntParentCatalogGroupRelationship"+Country_Suffix+".csv");
		outputCountry.delete(0, outputCountry.length());
		outputCountry.append("PartNumber"+","+"ParentGroupIdentifier"+","+"ParentStoreIdentifier"+","+"Sequence"+","+"Delete");
		outCountry.write(outputCountry.toString());
		outCountry.newLine();
		outCountry.flush();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatEntParentCatalogGroupRelationship"+Country_Suffix+" CSV ended");
		log.info(" ");
		log.info(" ");
		
// Below are for creating CatalogEntryProductCode.csv	
		catalogentryproductcodefw = new FileWriter(outputFileProdCode);
		outProdCode = new BufferedWriter(catalogentryproductcodefw);
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryProductCode for CatalogEntryProductCode.csv");
        outputProdCode.append("CatalogEntryProductCode");
        outProdCode.write(outputProdCode.toString());
        outProdCode.newLine();
        outProdCode.flush();
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for CatalogEntryProductCode.csv");
		outputProdCode.delete(0, outputProdCode.length());
		outputProdCode.append("PartNumber"+","+"Field4"+","+"Delete");
		outProdCode.write(outputProdCode.toString());
		outProdCode.newLine();
		outProdCode.flush();
        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in CatalogEntryProductCode CSV ended");
        log.info(" ");
        log.info(" ");
		
				
		}
		catch(Exception e)
		{
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught in createOutputFilesMethod \n"+e.getMessage());
		}
}

	   
//createOutputFilesSales method is used for creating headers to CatalogEntryParentCatalogGroupRelationship.csv file(Files will be used for loading in SalesCatalog Specific to Store)	   
	   public void createOutputFilesSales(String outputFileSales1, String outputFileSalesDelete1)
	{
		   outputFileSales=outputFileSales1;
		   outputFileSalesDelete=outputFileSalesDelete1;
		 
		try
		{

		// Below are for creating CatEntParentCatalogGroupRelationship_****.csv	
	    	catentparentcatalogsalesfw = new FileWriter(outputFileSales);
	    	outSales = new BufferedWriter(catentparentcatalogsalesfw);
	    	outputSales.delete(0, outputSales.length());
	    	log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryParentCatalogGroupRelationship for "+outputFileSales);
	        outputSales.append("CatalogEntryParentCatalogGroupRelationship");
	        outSales.write(outputSales.toString());
	        outSales.newLine();
	        outSales.flush();
	        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for "+outputFileSales);
	    	outputSales.delete(0, outputSales.length());
	    	outputSales.append("PartNumber"+","+"ParentGroupIdentifier"+","+"ParentStoreIdentifier"+","+"Sequence"+","+"Delete");
	    	outSales.write(outputSales.toString());
	    	outSales.newLine();
	    	outSales.flush();
	        log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in "+outputFileSales+" ended");
	        log.info(" ");
	        log.info(" "); 
	        	       	

        	// Below are for creating CatEntParentCatalogGroupRelationshipDelete_****.csv	
    		catentparentcatalogsalesdeletefw = new FileWriter(outputFileSalesDelete);
    		outSalesDelete = new BufferedWriter(catentparentcatalogsalesdeletefw);
    		outputSalesDelete.delete(0, outputSalesDelete.length());
    		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating Header CatalogEntryParentCatalogGroupRelationship for "+outputFileSalesDelete);
            outputSalesDelete.append("CatalogEntryParentCatalogGroupRelationship");
            outSalesDelete.write(outputSalesDelete.toString());
            outSalesDelete.newLine();
            outSalesDelete.flush();
            log.info(new Timestamp(date.getTime())+"		INFORMATION:	Creating column names for "+outputFileSalesDelete);
    		outputSalesDelete.delete(0, outputSalesDelete.length());
    		outputSalesDelete.append("PartNumber"+","+"ParentGroupIdentifier"+","+"ParentStoreIdentifier"+","+"Sequence"+","+"Delete");
    		outSalesDelete.write(outputSalesDelete.toString());
    		outSalesDelete.newLine();
    		outSalesDelete.flush();
            log.info(new Timestamp(date.getTime())+"		INFORMATION:	Created columns in "+outputFileSalesDelete+" ended");
            log.info(" ");
            log.info(" ");	
		
		
		}
		catch(Exception e)
		{
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught in createOutputFilesSales Method \n"+e.getMessage());
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
	    sabrixcodecategory1 = prop.getProperty("sabrixcodecategory1");
	    sabrixcodecategory2 = prop.getProperty("sabrixcodecategory2");
	    sabrixproductcode1 = prop.getProperty("sabrixproductcode_1");
	    sabrixproductcode2 = prop.getProperty("sabrixproductcode_2");
		langId = prop.getProperty("langId");
		langId_other = prop.getProperty("langId_other");
		inputFile = prop.getProperty("inputFile_catprod");
		store = prop.getProperty("store");
		suffix = prop.getProperty("suffix");
		outputFile = prop.getProperty("outputFile_catprod");
		outputFileProdCode = prop.getProperty("outputFile_catprodcode");
		ItemBean = prop.getProperty("item");
		Datesuffix = prop.getProperty("datesuffix");
		ProductBean = prop.getProperty("product");
		sabcodecat = sabrixcodecategory1.split(",");
		Country_Suffix = prop.getProperty("country_suffix");
		outputFileCountry = prop.getProperty("outputFile_catprod_country");
		
		productExclusionFile= prop.getProperty("inputFile_prodexclusion");
		checkFile(new File(productExclusionFile));
		outputFileBusiness = prop.getProperty("outputFile_catprod_business");
		outputFileBusinessDelete = prop.getProperty("outputFile_catproddel_business");
		Business_Suffix = prop.getProperty("business_suffix");
		if (countryLoad.equals("US"))
		{			
			outputFileBeneplace = prop.getProperty("outputFile_catprod_beneplace");
			outputFileBeneplaceDelete = prop.getProperty("outputFile_catproddel_beneplace");
			Beneplace_Suffix = prop.getProperty("beneplace_suffix");
		}
		
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
public void setConfigFileName(String log4JPropertyFile1, String propFileName1, String countryLoad1)
{
	log4JPropertyFile = log4JPropertyFile1;
	propFileName = propFileName1;
	countryLoad = countryLoad1;
}

//setProductExlusionList is for setting product exclusion specific to stores
public void setProductExlusionList()
{
	try
	 {
	  String line = null;		  
	  in = new FileReader(productExclusionFile);
	  BufferedReader br = new BufferedReader(in);
	  while((line = br.readLine())!=null){
		  Scanner s = new Scanner(line).useDelimiter(",");
		  productNumber=s.next();
		  productStore=s.next();
		  if(productStore.equalsIgnoreCase("Business"))
		  {
		  	BusinessExclusionList.add(productNumber);
		  }
		  else if(productStore.equalsIgnoreCase("Beneplace"))
		  {
		    BeneplaceExclusionList.add(productNumber);  
		  }
	    }
	 
	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Product exclusion partnumber's added to specific store list");
	  log.info(" ");
	  in.close();
	  br.close();
	  
	
	 }	
		
	 catch (Exception e) {
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while processing product exclusion input file "+e.getMessage());
			e.printStackTrace();
	}
}

//checkFile method is for checking the file existence and file permission
public void checkFile(File file){
    if (!file.exists()) 
    {
    	log.error(new Timestamp(date.getTime())+"		ERROR:	The file "+file+" does not exist");
     System.exit(1);
    }
        if (!file.canRead())
        {
        	log.error(new Timestamp(date.getTime())+"		ERROR:	The file "+file+" is not readable");
        System.exit(2);
        }
        }

}
