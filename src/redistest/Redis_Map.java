package redistest;

//cc MaxTemperatureMapper Mapper for maximum temperature example
//vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Redis_Map
extends Mapper<Text, Text, Text, Text> {//입력키, 입력값, 출력키, 출력값

	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

			context.write(new Text("a"), value);
	}
}
//^^ MaxTemperatureMapper

