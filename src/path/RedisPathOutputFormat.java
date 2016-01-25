package path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import redis.clients.jedis.Jedis;

//This output format class is templated to accept a key and value of type Text
public class RedisPathOutputFormat extends OutputFormat<Text, ObjectWritable> {

	//These static conf variables and methods are used to modify the job configuration.  This is a common pattern for MapReduce related classes to avoid the magic string problem
	public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
	static String DBhost;
	public static final String REDIS_HOSTS_CONF_DB = "mapred.redishashoutputformat.hosts_DB";
	
	public static void setRedisHosts(Job job, String hosts) {
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
		System.out.println("	-	output path : "+ hosts);
	}
	public static void setRedisHosts_DB(Job job, String hosts) {
		DBhost = hosts;
		job.getConfiguration().set(REDIS_HOSTS_CONF_DB, hosts);
		System.out.println("	-	DB path : "+ hosts);
	}
	
	//This method returns an instance of a RecordWriter for the task.  Note how we are pulling the variables set by the static methods during configuration
	public RecordWriter<Text, ObjectWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		String csvHosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		return new RedisHashRecordWriter(csvHosts);
	}

	//This method is used on the front-end prior to job submission to ensure everything is configured correctly
	public void checkOutputSpecs(JobContext job) throws IOException {
		String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		if (hosts == null || hosts.isEmpty()) {
			throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
		}

	}

	//The output committer is used on the back-end to, well, commit output.  Discussion of this class is out of scope, but more info can be found here
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		//use a null output committer, since
		return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
	}

	// This class is template to write only Text keys and Text values
	public static class RedisHashRecordWriter extends RecordWriter<Text, ObjectWritable> {

		// This map is used to map an integer to a Jedis instance
		String host;
		Jedis jedis;
		
		// This is the name of the Redis hash

		public RedisHashRecordWriter(String hosts) {
			this.host = hosts;
			// Create a connection to Redis for each host

		}
		
		public void check_bounce(ArrayList<String> bounce){
			@SuppressWarnings("resource")
			Jedis jedis_touhch = new Jedis(DBhost, 3389);//터치데이터디비
			jedis_touhch.select(8);//페이지 디비. 0? 3?
			
			for(String key : bounce){
				System.out.println("bounce key = " + key);
				jedis_touhch.hset(key, "bounce", "true");
			}
		}
		public void check_close(ArrayList<String> close){
			@SuppressWarnings("resource")
			Jedis jedis_touhch = new Jedis(DBhost, 3389);//터치데이터디비
			jedis_touhch.select(8);//페이지 디비. 0? 3?
			
			for(String key : close){
				System.out.println("close key = " + key);
				jedis_touhch.hset(key, "close", "true");
			}
		}
		public void check_destination(ArrayList<Convergence_Data> destination){
			@SuppressWarnings("resource")
			Jedis jedis_touhch = new Jedis(DBhost, 3389);//터치데이터디비
			jedis_touhch.select(8);//페이지 디비. 0? 3?
			for(Convergence_Data key : destination){
				System.out.println("destination key = " + key.touchdatakey + " time = " + ((Long)(key.elapsedtime)).toString() + "~@$%$#@!@$^%@!~!^&#@!%&@!~@$%^%!$^#@!~@%^#@$^&*&#");
				jedis_touhch.hset((key.touchdatakey.split(",")[1]).split("\\|")[0], "destination_elapsed_time", ((Long)(key.elapsedtime)).toString());
				jedis_touhch.hset((key.touchdatakey.split(",")[1]).split("\\|")[0], "destination_totalcount", ((Integer)(key.totalcount)).toString());
			}
		}
		public void check_timehost(ArrayList<Convergence_Data> timehost){
			@SuppressWarnings("resource")
			Jedis jedis_touhch = new Jedis(DBhost, 3389);//터치데이터디비
			jedis_touhch.select(8);//페이지 디비. 0? 3?
			for(Convergence_Data key : timehost){
				System.out.println("timehost key = " + key.touchdatakey);
				jedis_touhch.hset((key.touchdatakey.split(",")[1]).split("\\|")[0], "timehost_elapsed_time", ((Long)(key.elapsedtime)).toString());
				jedis_touhch.hset((key.touchdatakey.split(",")[1]).split("\\|")[0], "timehost_totalcount", ((Integer)(key.totalcount)).toString());
			}
		}
		public void check_timepage(ArrayList<Convergence_Data> timepage){
			@SuppressWarnings("resource")
			Jedis jedis_touhch = new Jedis(DBhost, 3389);//터치데이터디비
			jedis_touhch.select(8);//페이지 디비. 0? 3?
			for(Convergence_Data key : timepage){
				System.out.println("timepage key = " + key.touchdatakey);
				jedis_touhch.hset((key.touchdatakey.split(",")[1]).split("\\|")[0], "timepage_elapsed_time", ((Long)(key.elapsedtime)).toString());
				jedis_touhch.hset((key.touchdatakey.split(",")[1]).split("\\|")[0], "timepage_totalcount", ((Integer)(key.totalcount)).toString());
			}
		}
		public void check_events(ArrayList<Convergence_Data> events){
			@SuppressWarnings("resource")
			Jedis jedis_touhch = new Jedis(DBhost, 3389);//터치데이터디비
			jedis_touhch.select(8);//페이지 디비. 0? 3?
			for(Convergence_Data key : events){
				System.out.println("events key = " + key.touchdatakey);
				jedis_touhch.hset((key.touchdatakey.split(",")[1]).split("\\|")[0], "events_elapsed_time", ((Long)(key.elapsedtime)).toString());
				jedis_touhch.hset((key.touchdatakey.split(",")[1]).split("\\|")[0], "events_totalcount", ((Integer)(key.totalcount)).toString());
			}
		}
		
		//패스 데이터들을 실제로 메타디비에 넣기
		public void pushlogs(String key, String date, HashMap<String, HashMap<String, Integer>> pathlog){
			
			jedis.select(7);
		//System.out.println("~~~~~~~~~~~~~~~~~~~~init pushlogs~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

			Set<String> keyset = pathlog.keySet();
		//	String []logkeys = (String[]) keyset.toArray();
			HashMap<String, Integer> field;
			Set<String> fieldset;
			//String []logfields;
			String meta;
			int num;

			HashMap<String, String> input;

			//System.out.println("length of keyset = " + keyset.size());
			
			for(String logkeys : keyset){
			//	System.out.println("logkeys = " + logkeys);
				field = pathlog.get(logkeys);
				

				fieldset = field.keySet();
				//logfields = (String[]) fieldset.toArray();
				input = new HashMap<>();

				System.out.println("----~~~~~!!!key = "+key+"/:"+date+":"+logkeys);
				for(String logfields : fieldset){
					meta = jedis.hget(key+":"+date+":"+logkeys, logfields);
					num = pathlog.get(logkeys).get(logfields);
					if(meta != null)
						num += Integer.parseInt(meta);
					
					
					if(logkeys.split(",")[0].equals(logfields.split(",")[0])){
						continue;
					}
					System.out.println("value : " + logfields);
					
					input.put(logfields, ((Integer)num).toString());
					
				}
				if(input.size() != 0)
				jedis.hmset(key+"/:"+date+":"+logkeys, input);
			}			
			
		}


		// The write method is what will actually write the key value pairs out to Redis
		public void write(Text key, ObjectWritable value) throws IOException, InterruptedException {
			System.out.println("--------write");
			jedis = new Jedis(host, 4389);
			Result_Path result;
			
			if(value.getDeclaredClass() == Result_Path.class){//제대로 왔을때만 처리
				jedis.connect();
				result = (Result_Path) value.get();
				System.out.println("result = " + result.toString());
				System.out.println("result date = " + result.date);
				//System.out.println("result = " + result. );
				System.out.println("pathlog size = " + result.pathlog.size());
				//System.out.println("result = " + result.toString());
				//TODO : 현재 페이지 단위가 미구현!
				check_bounce(result.bounce);
				check_close(result.close);
				check_destination(result.destination);
				check_timehost(result.timehost);
				check_timepage(result.timepage);
				check_events(result.events);
				pushlogs(key.toString(),result.date, result.pathlog);
				
			}
			else{
				System.out.println("잘못된 결과입니다 - write");
			}
		}
		
		public void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
			// For each jedis instance, disconnect it
			//	jedis.disconnect();
		}
	} // end RedisRecordWriter
} // end RedisHashOutputFormat