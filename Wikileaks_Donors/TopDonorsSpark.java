package Bitcoin.Wikileaks_Donors;

import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
/**
 * Hello world!
 *
 */
public class TopDonorsSpark 
{
	
	//
	static final String filterPattern = "{blah blah }";
	
	
	static void initialiseSession(String tout, String tin, String OutputDest) {
		
		
		//This program is a spark version of my TopTen wikileaks hadoop program. In my Java Hadoop
		//version i first filtered lines in the tout text file using the Apache sparkk shell before feeding
		//The resulting file into cache becasue it was a simpel operation.
		//In this version i will start by performing the filtering logic inApp and will save this 
		//new text file to the HDFS first.
		
		//Pattern for performing filtering on idinitial dataset
		
		Pattern wikileaksFilter=Pattern.compile("{asfasdfa}");
		
		String logFile = "/home/lolo/Documents/Andria/Big_Data/Spark/example.txt"; // Should be some file on your system
	    
	    SparkSession spark = SparkSession.builder()
	    		             .appName("Simple Application")
	    		             .getOrCreate();
	    
	    JavaRDD<String> initialLines= spark.read().textFile(tout).javaRDD();
	    
	    JavaRDD<String> afterLines= initialLines.(line -> isDefined)
	    
	    
	   
	    
		
		
		
	}
	
	
	
	
	
	
	
	
    public static void main( String[] args )
    {
        if (args.length < 1) {
        	       System.err.println("Usage: JavaWordCount <file>");
           	       System.exit(1);
        	     }
        
        
        initialiseSession( args[0], args[1],args[2]);
    }
}
