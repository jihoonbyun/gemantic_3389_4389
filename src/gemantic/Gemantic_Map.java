package gemantic;

import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Gemantic_Map extends Mapper<Text, ObjectWritable, Text, ObjectWritable> {

	@Override
	public void map(Text key, ObjectWritable value, Context context) 
			throws IOException, InterruptedException {
	
		//��ġ ������
		if(value.getDeclaredClass() == PageData.class){
			PageData pdd;
			pdd = (PageData) value.get();
			context.write(new Text(pdd.url + "," + pdd.device), new ObjectWritable(pdd));
		}
		
		//�ܺ� ������
		if(value.getDeclaredClass() == ExternalData.class){
			ExternalData exData = (ExternalData) value.get();
			context.write(new Text(exData.url + "," + exData.device), new ObjectWritable(exData));
		}
		


	}
}