package POJO;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import Helper_Methods.Bitcoin_Converter;

public class TransactionOutWritable{
	
	
	String hash;
	
	String n;
	
	double value;
	
	String destinationAddress;
	
	
	
	public TransactionOutWritable(String [] words) {
		


		if( words.length > 0) {
			
			
			
		hash=(words[1]);	
			
	    value=Double.parseDouble(words[2]);
		 
		n=(words[3]);
		}
		else { 
			
			System.err.println("the word array is empty");
			
			
		}
	
	
	}


	
	public static TransactionOutWritable convertToTransactionOut(String line) throws IOException {
		
		return new TransactionOutWritable(line.split(","));


	
	}



	public Boolean isthisWikileaks(String RequiredAddress) {
		
		
		if ( this.destinationAddress.equals(RequiredAddress)) {
			return true;
		}
		return false;
	}

	


}
