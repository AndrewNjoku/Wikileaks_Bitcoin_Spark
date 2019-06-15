package com.andria.bitcoin.Wikileaks_Donors.POJO;

import java.io.IOException;

public class TransactionInWritable  implements java.io.Serializable{
		

		private static final long serialVersionUID = 1L;

		String tid;
		
		String hash;
		//matches n in tout
		String vout;
		
		

		
		public TransactionInWritable(String [] words) {
			


			if( words.length > 0) {


			    tid=words[0];
				
			    hash=(words[1]);

			    vout=(words[2]);
			}
			else { 
				
				System.err.println("the word array is empty");
				
				
			}
		
		
		}

		public static TransactionInWritable convertToTransactionIn(String line) throws IOException {
			
			return new TransactionInWritable(line.split(","));


		
		}

		public String getHash() {
			// TODO Auto-generated method stub
			return hash;
		}

		public String getID() {
			// TODO Auto-generated method stub
			return tid;
		}


		public String getVout() {
			// TODO Auto-generated method stub
			return vout;
		}


	}



