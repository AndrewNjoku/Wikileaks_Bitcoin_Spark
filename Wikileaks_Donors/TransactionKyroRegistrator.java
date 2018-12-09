package Bitcoin.Wikileaks_Donors;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import POJO.TransactionInWritable;
import POJO.TransactionOutWritable;
import POJO.TransactionsJoined;



public class TransactionKyroRegistrator {

	public void registerClasses(Kryo kryo) {
	
		//register class with Kyro for serialisation 
		
		kryo.register(TransactionOutWritable.class, new FieldSerializer(kryo, TransactionOutWritable.class));
		kryo.register(TransactionInWritable.class,new FieldSerializer(kryo, TransactionInWritable.class));
		kryo.register(TransactionsJoined.class, new FieldSerializer<>(kryo, TransactionsJoined.class));
		
		
	}

}