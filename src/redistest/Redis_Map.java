package redistest;

//cc MaxTemperatureMapper Mapper for maximum temperature example
//vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Redis_Map
extends Mapper<Text, Text, Text, Text> {//�Է�Ű, �Է°�, ���Ű, ��°�

	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

			context.write(new Text("a"), value);
	}
}
//^^ MaxTemperatureMapper

