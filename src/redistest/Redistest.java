package redistest;

//cc MaxTemperature Application to find the maximum temperature in the weather dataset
//vv MaxTemperature

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class Redistest {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Redistest <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(Redistest.class);
		job.setJobName("Redis");

		//레디스 접속 모듈
		job.setInputFormatClass(RedisHashInputFormat.class);
		RedisHashInputFormat.setRedisHosts(job, "127.0.0.1");
		RedisHashInputFormat.setRedisHashKey(job, "gemantic");

		//레디스 접속 모듈
		job.setOutputFormatClass(RedisHashOutputFormat.class);
		RedisHashOutputFormat.setRedisHosts(job, "127.0.0.1");
		RedisHashOutputFormat.setRedisHashKey(job, "test");
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Redis_Map.class);
		job.setReducerClass(Redis_Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
//^^ MaxTemperature


