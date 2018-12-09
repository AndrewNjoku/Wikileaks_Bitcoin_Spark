package Bitcoin.Wikileaks_Donors;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.serializer.KryoSerializer;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import POJO.TransactionInWritable;
import POJO.TransactionOutWritable;
import scala.Tuple2;
/**
 * Hello world!
 *
 */
public class TopDonorsSpark 
{
	
	//
	static final String requiredBitcoinAddress= "{ blah blah }";
	
	static JavaPairRDD<String,TransactionOutWritable> toutRepo;
	
	static JavaPairRDD<String,TransactionInWritable> tinRepo;
	
	//static JavaPairRDD<String,TransactionOut>hashRepo;
	
	
	static void filterTransactionsAndCache(SparkSession context, String tout) {
		
		
		//This program is a spark version of my TopTen wikileaks hadoop program. In my Java Hadoop
		//version i first filtered lines in the tout text file using the Apache sparkk shell before feeding
		//The resulting file into cache becasue it was a simpel operation.
		//In this version i will start by performing the filtering logic inApp and will save this 
		//new text file to the HDFS first.
		
		//Pattern for performing filtering on idinitial dataset
		
	
	    
	    JavaRDD<String> AllLines= context.read().textFile(tout).javaRDD();
	    
	    //perform the conversion to inflate my pojo
	    
	    
	    JavaRDD<String>tfiltered = AllLines.filter(line -> line.contains(requiredBitcoinAddress));
	    
	    
	    //This is my repo , now all i have to do is compare the keys to make the join
	    
	    toutRepo=tfiltered.mapToPair(new PairFunction<String, String, TransactionOutWritable>() {
	    	
	    	@Override
            public Tuple2<String,TransactionOutWritable> call(String s) throws Exception {
                String[] words = s.split(",");
                
                TransactionOutWritable tout = TransactionOutWritable.convertToTransactionOut(s);
             
                //Using the hash as a key for quick lookup
                
                return new Tuple2(words[0], tout);
            }

	    	
	    	
		}).cache();
			                                 
	   
	    
	  
	    

		
	}
	private static void readTransInandCreateRDD(SparkSession context, String tin) {
		
		  JavaRDD<String> AllLines= context.read().textFile(tin).javaRDD();
		    
		    //perform the conversion to inflate my pojo
		    
		  JavaRDD<String>Keys =toutRepo.keys();
		   
		  List<String > keys = Keys.collect();
		  
		  
		    //Filter to only lines that contain matching hashes 
		  JavaRDD<String>tfiltered = AllLines.filter( line -> line.contains(keys.iterator().next()));
		    
		    
		    //This is my repo , now all i have to do is compare the keys to make the join
		    
		  tinRepo=tfiltered.mapToPair(new PairFunction<String, String, TransactionInWritable>() {
		    	

		    	@Override
	            public Tuple2<String,TransactionInWritable> call(String s) throws Exception {
	                String[] words = s.split(",");
	                
	                TransactionInWritable tin = TransactionInWritable.convertToTransactionIn(s);
	             
	                return new Tuple2(words[1], tin);
	            }

		    	
			}).cache();		
		
	}
	
    private static void JoinDatasetsOutputFile(SparkSession context) {
		
		// we will read in the tin file and compare its hash to the hashes in our cached RDD
    	
    	
		
		JavaPairRDD<String , Tuple2<TransactionOutWritable,TransactionInWritable>> JoinedMatey= toutRepo.join(tinRepo);
		
		
		
		
	    
		
		
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
       		                 	            .set("spark.kryo.registrator",TransactionKyroRegistrator.class.getName());
       		                                
       		                              
        SparkSession spark = SparkSession.builder()
        		                           .config(myConfig)
        		                           .getOrCreate();
        
        
       // spark.sparkContext().parallelize(seq, numSlices, evidence$1)
        		                           
        		 
        		 
        		 
        		
	            
        
        //First we filter and cache output 
        filterTransactionsAndCache(spark,args[0]);
        
        //Do the same for Tin, We have filtered resuts so safe to cache both. Caching is faster!
        readTransInandCreateRDD(spark,args[1]);
        
        //This is similiar to the Maps initialise step
        JoinDatasetsOutputFile(spark);
        
    }

	


	


	
}
