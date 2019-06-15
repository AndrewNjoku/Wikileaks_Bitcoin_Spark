package com.andria.bitcoin.Wikileaks_Donors.POJO;



import java.io.IOException;




public class TransactionOutWritable implements java.io.Serializable{
	

	private static final long serialVersionUID = 1L;

	private String hash;
	
	private String n;
	
	private double value;
	
	private String destinationAddress;
	
	


	private TransactionOutWritable(String[] words) {
		


		if( words.length > 0) {
			
			
			
		hash=(words[1]);	
			
	    value=Double.parseDouble(words[2]);
		 
		n=(words[3]);
		}
		else { 
			
			System.err.println("the word array is empty");
			
			
		}
	
	
	}


    public String getHash() {
        return hash;
    }



    public Boolean isthisWikileaks(String RequiredAddress) {
		
		
		if ( this.destinationAddress.equals(RequiredAddress)) {
			return true;
		}
		return false;
	}
	

	public static TransactionOutWritable convertToTransactionOut(String line) throws IOException {
		
		return new TransactionOutWritable(line.split(","));


	
	}





	public double getBtc() {
		// TODO Auto-generated method stub
		return value;
	}






	


}
