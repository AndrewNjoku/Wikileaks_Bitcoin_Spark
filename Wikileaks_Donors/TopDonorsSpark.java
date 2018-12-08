package Bitcoin.Wikileaks_Donors;

import java.util.regex.Pattern;
import org.apache.spark.serializer.KryoSerializer;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import POJO.TransactionOutWritable;
/**
 * Hello world!
 *
 */
public class TopDonorsSpark 
{
	
	//
	static final String requiredBitcoinAddress= "{blah blah }";
	
	static JavaPairRDD<String,TransactionOut>hashRepo;
	
	
	static void filterTransactionsAndCache(SparkSession context, String tout) {
		
		
		//This program is a spark version of my TopTen wikileaks hadoop program. In my Java Hadoop
		//version i first filtered lines in the tout text file using the Apache sparkk shell before feeding
		//The resulting file into cache becasue it was a simpel operation.
		//In this version i will start by performing the filtering logic inApp and will save this 
		//new text file to the HDFS first.
		
		//Pattern for performing filtering on idinitial dataset
		
		Pattern wikileaksFilter=Pattern.compile("{asfasdfa}");
	
	    
	    JavaRDD<String> AllLines= context.read().textFile(tout).javaRDD();
	    
	    //perform the conversion to inflate my pojo
	    
	    
	   JavaRDD<TransactionOutWritable> out = AllLines.map(lines -> TransactionOutWritable
			                                 .convertToTransactionOut(lines))
			                                 .filter(new Function<TransactionOutWritable, Boolean>() {
			                             		
			                         			private static final long serialVersionUID = 1L;
			                         			private String wikileaksBitcoinAddress = "{blah}";
			                         	    
	
			                         			//cache this RDD since accessign cache will be much more efficient than accessing the HDFS 
			                         			//again since network is the number one bottleneck in this application and the file is small 
			                         			//enough to be stored in workign memory

												@Override
												public Boolean call(TransactionOutWritable t) throws Exception {
													
													return t.isthisWikileaks(requiredBitcoinAddress);
												}
			                         			
			                         		}).cache();
			                                 
	    
	  
	    

		
	}
	

    public static void main( String[] args )
    {
        if (args.length < 1) {
        	       System.err.println("Usage: JavaWordCount <file>");
           	       System.exit(1);
        	     }
        
        //Start the session and instantiate the context we will be workign with 
        
        
        
        SparkConf myConfig = new SparkConf().setAppName("Simple Application")
       		                                .setMaster("local")
       		                                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       		                 	            .set("spark.kryo.registrator",TransactionOutWritable.class.getName());
       		                                
       		                              
        SparkSession spark = SparkSession.builder()
        		                           .config(myConfig)
        		                           .getOrCreate();
        		                           
        		 
        		 
        		 
        		
	             
	             .getOrCreate();
        
        //First we filter and cache output 
        filterTransactionsAndCache(spark,args[0]);
        
        //This is similiar to the Maps initialise step
        
        inflateRepo();
        
        //Now we perform the "MAP", we will inut the tout join with cache and perform a sort on this 
        //Dataset
        
        JoinDatasets(spark,args[1]);
        
    }


	private static void inflateRepo(SparkSession a) {
		// TODO Auto-generated method stub
		
		a.sparkContext().pa
		
	}


	private static void JoinDatasets(SparkSession context, String tin) {
		
		// To begin with we will 
		
		JavaRDD<String> tinLines= context.read().textFile(tin).javaRDD();
		
		
	    
		
		
	}
}
