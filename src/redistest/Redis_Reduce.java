package redistest;

//cc MaxTemperatureReducer Reducer for maximum temperature example
//vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Redis_Reduce
extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context)
					throws IOException, InterruptedException {

		for (Text value : values) {
			context.write(key, value);
		}
	}
}
//^^ MaxTemperatureReducer

