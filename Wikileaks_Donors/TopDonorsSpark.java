package Bitcoin.Wikileaks_Donors;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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
	
	
	static void filterTransactionsAndCache(SparkSession context, String tout) {
		
		
		//This program is a spark version of my TopTen wikileaks hadoop program. In my Java Hadoop
		//version i first filtered lines in the tout text file using the Apache sparkk shell before feeding
		//The resulting file into cache becasue it was a simpel operation.
		//In this version i will start by performing the filtering logic inApp and will save this 
		//new text file to the HDFS first.
		
		//Pattern for performing filtering on idinitial dataset
		
		Pattern wikileaksFilter=Pattern.compile("{asfasdfa}");
	
	    
	    JavaRDD<String> AllLines= context.read().textFile(tout).javaRDD();
	    
	    JavaRDD<String> WikileaksTransactions= AllLines.filter(new Function<String, Boolean>() {
		
			private static final long serialVersionUID = 1L;
			private String wikileaksBitcoinAddress = "{blah}";
	    
			public Boolean call(String line) throws Exception {
				
				return line.contains(wikileaksBitcoinAddress);
			}
			//cache this RDD since accessign cache will be much more efficient than accessing the HDFS 
			//again since network is the number one bottleneck in this application and the file is small 
			//enough to be stored in workign memory
			
		}).cache();
	    

		
	}
	

    public static void main( String[] args )
    {
        if (args.length < 1) {
        	       System.err.println("Usage: JavaWordCount <file>");
           	       System.exit(1);
        	     }
        
        //Start the session and instantiate the context we will be workign with 
        
        SparkSession spark = SparkSession.builder()
	             .appName("Simple Application")
	             .getOrCreate();
        
        //First we filter and cache output 
        filterTransactionsAndCache(spark,args[0]);
    }
}
