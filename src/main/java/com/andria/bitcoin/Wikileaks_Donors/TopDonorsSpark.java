package com.andria.bitcoin.Wikileaks_Donors;


//Basics
import java.io.File;
import java.io.IOException;
import java.util.List;



//Specialisations
import com.andria.bitcoin.Wikileaks_Donors.POJO.TransactionInWritable;
import com.andria.bitcoin.Wikileaks_Donors.POJO.TransactionOutWritable;
import com.andria.bitcoin.Wikileaks_Donors.POJO.TransactionsJoined;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;



public class TopDonorsSpark 
{
	
	//Set the amount of donors you want returned here
	
	static final int DonorNo = 10;
	
	static final String outputFile="/Andria/Output/";
	//
	static final String requiredBitcoinAddress= "{ blah blah }";
	
	static JavaPairRDD<String, TransactionOutWritable> toutRepo;
	static JavaPairRDD<String, TransactionInWritable> tinRepo;
	
	

	static void filterTransactionsAndCache(SparkSession context, String tout) {

	    
	    JavaRDD<String> AllLines= context.read().textFile(tout).javaRDD();
	    
	    //perform the conversion to inflate my pojo
	    
	    
	    JavaRDD<String>tfiltered = AllLines.filter(line -> line.contains(requiredBitcoinAddress));
	    
	    
	    //This is my repo , now all i have to do is compare the keys to make the join
	    
	    toutRepo=tfiltered.mapToPair(new PairFunction<String, String, TransactionOutWritable>() {
	    	
	    	@Override
            public Tuple2<String,TransactionOutWritable> call(String s) throws Exception {
               //Again i dont need this string aray keep stuff OOP String[] words = s.split(",");
                
                TransactionOutWritable tout = TransactionOutWritable.convertToTransactionOut(s);
             
                //Using the hash as a key for quick lookup
                
                return new Tuple2(tout.getHash(), tout);
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

        //we can also cache this much smaller dataset to speed things up

        tinRepo=tfiltered.mapToPair((PairFunction<String, String, TransactionInWritable>) s -> {
// String[] words = s.split(",");

TransactionInWritable tin1 = TransactionInWritable.convertToTransactionIn(s);


//The hash is stored in [1] this time

return new Tuple2(tin1.getHash(), tin1);
}).cache();
		
	}
	
    private static void JoinDatasetsOutputFile(SparkSession context) throws IOException {

    	JavaPairRDD<String , Tuple2<TransactionOutWritable,TransactionInWritable>> JoinedMatey;

		JoinedMatey= toutRepo.join(tinRepo).cache();
		
		JavaPairRDD<String, TransactionsJoined>joinTransactionObjects =JoinedMatey.mapValues(x -> TransactionsJoined.newTransactionsJoined(x._2,x._1));
		//We are sorting based on a call to sortBy in which we have an anonymous inner function to extract the bitcoin value in order to
		//sort
		JavaRDD<TransactionsJoined>FlattenTransaction=joinTransactionObjects.values().sortBy(new Function<TransactionsJoined,Double>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Double call(TransactionsJoined v1) throws Exception {

				return v1.getBtc();
			}
			//Change for controlling ascending descending etc
		},true,1);

		List<TransactionsJoined> top10 = FlattenTransaction.top(10);

		FileUtils.writeLines(new File(outputFile), null, top10);
	
    }


    public static void main( String[] args ) throws IOException
    {
        if (args.length < 1) {
        	       System.err.println("need more args");
           	       System.exit(1);
        	     }
        
        //Start the session and instantiate the context we will be workign with 
        
        
        //Set to local since running on local machine as master
        
        SparkConf myConfig = new SparkConf().setAppName("Wiki_Donors")
       		                                .setMaster("local")
       		                                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       		                 	            .set("spark.kryo.registrator",TransactionKyroRegistrator.class.getName());
       		                                
       		                              
        SparkSession spark = SparkSession.builder()
        		                           .config(myConfig)
        		                           .getOrCreate();
     
        
        //First we filter and cache output 
        filterTransactionsAndCache(spark,args[0]);
        
        //Do the same for Tin, We have filtered resuts so safe to cache both. Caching is faster!
        readTransInandCreateRDD(spark,args[1]);
        
        //This is similiar to the Maps initialise step
        try {
			JoinDatasetsOutputFile(spark);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
    }

	


	


	
}
