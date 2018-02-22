//********************************************************************************************
// The purpose of this code is to call handler code for creating WCS7 understandable CSVs
// Author: testin
// Email: vigneven@in.ibm.comtesting
// Version: 1.0
//********************************************************************************************

package com.ibm.commerce.stella.dataload.handler;


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.InputSource;

public class GenParserIcpe {
		private static String handlerlist=null;
		private static String inputFileName_proddesc=null;
		private static String inputFileName_prod=null;
		private static String inputFileName_pric=null;
		private static String inputFileName_catprod=null;
		private static String inputFileName_prodctry=null;
		private static String inputFileName_attribute=null;
		private static String inputFileName_attributevalue=null;
		private static String inputFileName_compat=null;
		private static String inputFileName_prodfee=null;
		private static String inputFileName_delete=null;
		private static String globalFile= null;
		private static String log4JPropertyFile;
		private static String propFileName=null;
		private static String countryLoad=null;
//		Below are log4j related	   
		// Logger log = Logger.getLogger(this.getClass());
		
		static Logger log= Logger.getLogger("genParserLogger");	   
		static Date date = new Date();
		
	public static void main(String[] args){
		
		String[] handler = null;
		
		log4JPropertyFile = "C:/Users/SaiKumarVemula/sai/StellaICPE/src/com/ibm/commerce/stella/dataload/utility/log4j.properties";
		//propFileName = "C:/Users/SaiKumarVemula/sai/StellaICPE/src/com/ibm/commerce/stella/dataload/utility/US/Properties.properties";
		propFileName = "C:/Users/SaiKumarVemula/sai/StellaICPE/src/com/ibm/commerce/stella/dataload/utility/CA/Properties.properties";
		globalFile="C:/Users/SaiKumarVemula/sai/StellaICPE/conf/Global.ini";
		//countryLoad = "GEO";
		//countryLoad = "US";
		countryLoad = "CA";
		
		/*
		if(args.length<4 || args.length>4)
		{
			log.error(new Timestamp(date.getTime())+"		ERROR:	Proper Usage is-- java program filename [logpath] [propertiespath] [conffile] [countryload]");
			System.exit(1);
		}
		else
		{
		log4JPropertyFile = args[0];
		propFileName = args[1];
		globalFile = args[2];
		countryLoad = args[3];
		}
		*/
		GenParserIcpe g1= new GenParserIcpe();
		g1.globalRead();
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting GenParser handler");
		log.info(" ");
		log.info(" ");
		handler = handlerlist.split(",");
		for (int i =0;i<handler.length;i++)
		{	
		  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Calling handler :  "+handler[i]);
	      callHandler(handler[i]);
	      log.info(new Timestamp(date.getTime())+"		INFORMATION:	Ending handler :  "+handler[i]);
	      log.info(" ");
		  log.info(" ");
		}
		log.info(new Timestamp(date.getTime())+"		INFORMATION:	Completed GenParser handler");  
	   }   
	private static void callHandler(String handler1)
	{
		String handler=handler1;
		try {	
			  //ProductDesc handler
	    	  if (handler.equals("PRODUCTDESC"))
	    	  {
	    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting ProductDesc Handler");
	    	  ProductDescHandlerIcpe productdeschandler = new ProductDescHandlerIcpe();
	    	  productdeschandler.setConfigFileName(log4JPropertyFile,propFileName);
	    	  inputFileName_proddesc = productdeschandler.propertiesRead();
	          productdeschandler.createOutputFiles();
	          File inputFile_proddesc = new File(inputFileName_proddesc);
	          InputStream inputStream_proddesc= new FileInputStream(inputFile_proddesc);
	          Reader reader_proddesc = new InputStreamReader(inputStream_proddesc,"UTF-8");
	      	  InputSource is = new InputSource(reader_proddesc);
	          is.setEncoding("UTF-8");
	          SAXParserFactory factory_proddesc = SAXParserFactory.newInstance();
	          SAXParser saxParser_proddesc = factory_proddesc.newSAXParser();
	          saxParser_proddesc.parse(is, productdeschandler);  
	          log.info(new Timestamp(date.getTime())+"		INFORMATION:	ProductDesc handler completed");
	          
	    	  }
			//Product handler
	    	  else if (handler.equals("PRODUCT"))
	    	  {
	    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting Product Handler");
	    	  ProductHandlerIcpe producthandler = new ProductHandlerIcpe();
	    	  producthandler.setConfigFileName(log4JPropertyFile,propFileName);
	    	  inputFileName_prod = producthandler.propertiesRead();
	          producthandler.createOutputFiles();
	          File inputFile_prod = new File(inputFileName_prod);
	          InputStream inputStream_prod= new FileInputStream(inputFile_prod);
	          Reader reader_prod = new InputStreamReader(inputStream_prod,"UTF-8");
 	      	  InputSource is1 = new InputSource(reader_prod);
 	          is1.setEncoding("UTF-8");
	          SAXParserFactory factory_prod = SAXParserFactory.newInstance();
	          SAXParser saxParser_prod = factory_prod.newSAXParser();
	          saxParser_prod.parse(is1, producthandler);  
	          log.info(new Timestamp(date.getTime())+"		INFORMATION:	Product handler completed");
	          
	    	  }	         
	         
	         //Price handler
	    	  else if (handler.equals("PRICE"))
	    	  {
	    	 log.info(new Timestamp(date.getTime())+"		INFORMATION:	Staring Price Handler");
	         PriceHandlerIcpe pricehandler = new PriceHandlerIcpe();
	    	 pricehandler.setConfigFileName(log4JPropertyFile,propFileName);
	    	 inputFileName_pric = pricehandler.propertiesRead();
	         pricehandler.createOutputFiles();
	         File inputFile_pric = new File(inputFileName_pric);
	         InputStream inputStream_pric= new FileInputStream(inputFile_pric);
	         Reader reader_pric = new InputStreamReader(inputStream_pric,"UTF-8");
	         InputSource is2 = new InputSource(reader_pric);
	         is2.setEncoding("UTF-8");
	         SAXParserFactory factory_pric = SAXParserFactory.newInstance();
	         SAXParser saxParser_pric = factory_pric.newSAXParser();
	         saxParser_pric.parse(is2, pricehandler);
	         log.info(new Timestamp(date.getTime())+"		INFORMATION:	Price Handler Completed");
	    	  }
	    	//CategoryProduct handler
	         else if (handler.equals("CATPRODREL"))
	    	  {
	         log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting CategoryProduct Handler");
	         CatProdHandlerIcpe catprodhandler = new CatProdHandlerIcpe();
	    	 catprodhandler.setConfigFileName(log4JPropertyFile,propFileName,countryLoad);
	    	 inputFileName_catprod = catprodhandler.propertiesRead();
	         catprodhandler.createOutputFiles();
	         File inputFile_catprod = new File(inputFileName_catprod);
	         InputStream inputStream_catprod= new FileInputStream(inputFile_catprod);
	         Reader reader_catprod = new InputStreamReader(inputStream_catprod,"UTF-8");
	         InputSource is3 = new InputSource(reader_catprod);
	         is3.setEncoding("UTF-8");
	         SAXParserFactory factory_catprod = SAXParserFactory.newInstance();
	         SAXParser saxParser_catprod = factory_catprod.newSAXParser();
	    	 saxParser_catprod.parse(is3, catprodhandler); 
	    	 log.info(new Timestamp(date.getTime())+"		INFORMATION:	CategoryProduct Handler Completed");	             
	    	  }
	    	//ProductCountry handler(Inventory)
	         else if (handler.equals("PRODCOUNTRY"))
	    	  {
	         log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting ProductCountry Handler(Inventory)");
	         ProdCtryHandlerIcpe prodctryhandler = new ProdCtryHandlerIcpe();
	    	 prodctryhandler.setConfigFileName(log4JPropertyFile,propFileName);
	    	 inputFileName_prodctry = prodctryhandler.propertiesRead();
	         prodctryhandler.createOutputFiles();
	         File inputFile_prodctry = new File(inputFileName_prodctry);
	         InputStream inputStream_prodctry= new FileInputStream(inputFile_prodctry);
	         Reader reader_prodctry = new InputStreamReader(inputStream_prodctry,"UTF-8");
	         InputSource is4 = new InputSource(reader_prodctry);
	         is4.setEncoding("UTF-8");
	         SAXParserFactory factory_prodctry = SAXParserFactory.newInstance();
	         SAXParser saxParser_prodctry = factory_prodctry.newSAXParser();
	    	 saxParser_prodctry.parse(is4, prodctryhandler); 
	    	 log.info(new Timestamp(date.getTime())+"		INFORMATION:	ProductCountry Handler(Inventory) Completed");	             
	    	  }
	    	//Attribute handler
	    	  else if (handler.equals("ATTRIBUTE"))
	    	  {
	    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting Attribute Handler");
	    	  AttributeHandlerIcpe attributehandler = new AttributeHandlerIcpe();
	    	  attributehandler.setConfigFileName(log4JPropertyFile,propFileName);
	    	  inputFileName_attribute = attributehandler.propertiesRead();
	          attributehandler.createOutputFiles();
	          File inputFile_attribute = new File(inputFileName_attribute);
	          InputStream inputStream_attribute= new FileInputStream(inputFile_attribute);
	          Reader reader_attribute = new InputStreamReader(inputStream_attribute,"UTF-8");
	      	  InputSource is5 = new InputSource(reader_attribute);
	          is5.setEncoding("UTF-8");
	          SAXParserFactory factory_attribute = SAXParserFactory.newInstance();
	          SAXParser saxParser_attribute = factory_attribute.newSAXParser();
	          saxParser_attribute.parse(is5, attributehandler);  
	          log.info(new Timestamp(date.getTime())+"		INFORMATION:	Attribute handler completed");	          
	    	  }
	    	//Attribute Value handler
	    	  else if (handler.equals("ATTRIBUTEVALUE"))
	    	  {
	    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting AttributeValue Handler");
	    	  AttributeValueHandlerIcpe attributevaluehandler = new AttributeValueHandlerIcpe();
	    	  attributevaluehandler.setConfigFileName(log4JPropertyFile,propFileName);
	    	  inputFileName_attributevalue = attributevaluehandler.propertiesRead();
	          attributevaluehandler.createOutputFiles();
	          File inputFile_attributevalue = new File(inputFileName_attributevalue);
	          InputStream inputStream_attributevalue= new FileInputStream(inputFile_attributevalue);
	          Reader reader_attributevalue = new InputStreamReader(inputStream_attributevalue,"UTF-8");
	      	  InputSource is6 = new InputSource(reader_attributevalue);
	          is6.setEncoding("UTF-8");
	          SAXParserFactory factory_attributevalue = SAXParserFactory.newInstance();
	          SAXParser saxParser_attributevalue = factory_attributevalue.newSAXParser();
	          saxParser_attributevalue.parse(is6, attributevaluehandler);  
	          log.info(new Timestamp(date.getTime())+"		INFORMATION:	AttributeValue handler completed");
	          
	    	  }
	    	//Compat handler
	    	  else if (handler.equals("COMPAT"))
	    	  {
	    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting Compat Handler");
	    	  CompatHandlerIcpe compathandler = new CompatHandlerIcpe();
	    	  compathandler.setConfigFileName(log4JPropertyFile,propFileName);
	    	  inputFileName_compat = compathandler.propertiesRead();
	    	  compathandler.createOutputFiles();
	          File inputFile_compat = new File(inputFileName_compat);
	          InputStream inputStream_compat= new FileInputStream(inputFile_compat);
	          Reader reader_compat = new InputStreamReader(inputStream_compat,"UTF-8");
	      	  InputSource is7 = new InputSource(reader_compat);
	          is7.setEncoding("UTF-8");
	          SAXParserFactory factory_compat = SAXParserFactory.newInstance();
	          SAXParser saxParser_compat = factory_compat.newSAXParser();
	          saxParser_compat.parse(is7, compathandler);  
	          log.info(new Timestamp(date.getTime())+"		INFORMATION:	Compat handler completed");
	          
	    	  }
	    	//Product fee handler
	    	  else if (handler.equals("PRODUCTFEE"))
	    	  {
	    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting ProductFee Handler");
	    	  ProductFeeHandlerIcpe prodfeehandler = new ProductFeeHandlerIcpe();
	    	  prodfeehandler.setConfigFileName(log4JPropertyFile,propFileName);
	    	  inputFileName_prodfee = prodfeehandler.propertiesRead();
	    	  prodfeehandler.createOutputFiles();
	          File inputFile_prodfee = new File(inputFileName_prodfee);
	          InputStream inputStream_prodfee= new FileInputStream(inputFile_prodfee);
	          Reader reader_prodfee = new InputStreamReader(inputStream_prodfee,"UTF-8");
	      	  InputSource is8 = new InputSource(reader_prodfee);
	          is8.setEncoding("UTF-8");
	          SAXParserFactory factory_prodfee = SAXParserFactory.newInstance();
	          SAXParser saxParser_prodfee = factory_prodfee.newSAXParser();
	          saxParser_prodfee.parse(is8, prodfeehandler);  
	          log.info(new Timestamp(date.getTime())+"		INFORMATION:	ProductFee handler completed");
	          
	    	  }
	    	  //Delete handler
	    	  else if (handler.equals("DELETE"))
	    	  {
	    	  log.info(new Timestamp(date.getTime())+"		INFORMATION:	Starting Delete Handler");
	    	  DeleteHandlerIcpe deletehandler = new DeleteHandlerIcpe();
	    	  deletehandler.setConfigFileName(log4JPropertyFile,propFileName);
	    	  inputFileName_delete = deletehandler.propertiesRead();
	    	  deletehandler.createOutputFiles();
	          File inputFile_delete = new File(inputFileName_delete);
	          InputStream inputStream_delete= new FileInputStream(inputFile_delete);
	          Reader reader_delete = new InputStreamReader(inputStream_delete,"UTF-8");
	      	  InputSource is9 = new InputSource(reader_delete);
	          is9.setEncoding("UTF-8");
	          SAXParserFactory factory_delete = SAXParserFactory.newInstance();
	          SAXParser saxParser_delete = factory_delete.newSAXParser();
	          saxParser_delete.parse(is9 , deletehandler);  
	          log.info(new Timestamp(date.getTime())+"		INFORMATION:	Delete handler completed");
	    	  }
	    	  else
	    	  {
	    		  log.error(new Timestamp(date.getTime())+"		ERROR:	Please check the handler name");
	    		  System.exit(1);
	    	  }
	      } catch (Exception e) {
	    	  log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while executing handler "+e.getMessage());
	    	  System.exit(1);
	      }     
	}
	public void globalRead()
	{
		try
		{
			Properties p = new Properties();
			p.load(new FileInputStream(log4JPropertyFile));
			PropertyConfigurator.configure(p);
			checkFile(new File(propFileName));
			checkFile(new File(globalFile));
			Properties prop = new Properties();
			InputStream inputStream;
			log.info(new Timestamp(date.getTime())+"		INFORMATION:	Reading data from Global file "+globalFile);
			inputStream = new FileInputStream(globalFile);
			if (inputStream != null) {
			prop.load(inputStream);
			}	
			if (countryLoad.equals("GEO"))
			{
			handlerlist = prop.getProperty("HANDLERLISTGEO");	
			}			
			else if(countryLoad.equals("US") || countryLoad.equals("CA"))
			{
				handlerlist = prop.getProperty("HANDLERLISTCOUNTRY");
			}
			else if(!countryLoad.equals("GEO") || !countryLoad.equals("US") || !countryLoad.equals("CA"))
			{
				log.error(new Timestamp(date.getTime())+"		ERROR:	Country Load should be GEO or US or CA in java syntax");
				System.exit(1);
			}
			log.info(new Timestamp(date.getTime())+"		INFORMATION:	Property file reading completed");
			log.info(" ");
			log.info(" ");
						
		} 
		catch(Exception e)
		{
			log.error(new Timestamp(date.getTime())+"		ERROR:	Exception caught while reading properties file "+e.getMessage());
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
