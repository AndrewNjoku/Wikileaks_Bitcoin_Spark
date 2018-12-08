package Bitcoin.Wikileaks_Donors;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import POJO.TransactionOutWritable;



public class TransactionKyroRegistrator {

	public void registerClasses(Kryo kryo) {
	
		// TODO Auto-generated method stub
		
		
		//register class with Kyro for serialisation 
		
		kryo.register(TransactionOutWritable.class, new FieldSerializer(kryo, TransactionOutWritable.class));
		
	}

}