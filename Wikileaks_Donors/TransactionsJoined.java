package Bitcoin.Wikileaks_Donors;

public class TransactionsJoined {
	
	String Hash;
	
	String tID;
	
	String n;
	
	String vout;
	
	double btc;
	
	
	
	
	public TransactionsJoined(TransactionInWritable t, TransactionOutWritable s) {
		
		Hash=t.getHash();
		tID=t.getID();
		vout=t.getVout();
		btc = s.getBtc();
		
		
		
		
	}
	
	
	
	
	
	
	
	public static TransactionsJoined newTransactionsJoined(TransactionInWritable tin, TransactionOutWritable tout) {
		
		
		return new TransactionsJoined( tin,tout);
		
		
		
	}







	public Double getBtc() {
		// TODO Auto-generated method stub
		return btc;
	}

}
