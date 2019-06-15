package com.andria.bitcoin.Wikileaks_Donors;

import com.andria.bitcoin.Wikileaks_Donors.POJO.TransactionInWritable;
import com.andria.bitcoin.Wikileaks_Donors.POJO.TransactionOutWritable;
import com.andria.bitcoin.Wikileaks_Donors.POJO.TransactionsJoined;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;



public class TransactionKyroRegistrator {

	public void registerClasses(Kryo kryo) {
	
		//register class with Kyro for serialisation 
		
		kryo.register(TransactionOutWritable.class, new FieldSerializer(kryo, TransactionOutWritable.class));
		kryo.register(TransactionInWritable.class,new FieldSerializer(kryo, TransactionInWritable.class));
		kryo.register(TransactionsJoined.class, new FieldSerializer<>(kryo, TransactionsJoined.class));
		
		
	}

}