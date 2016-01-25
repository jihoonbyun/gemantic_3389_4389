package redistest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import redis.clients.jedis.Jedis;

//This input format will read all the data from a given set of Redis hosts
public class RedisHashInputFormat_saved extends InputFormat<Text, Text> {

	//Again, the CSV list of hosts and a hash key variables and methods for configuration
	public static final String REDIS_HOSTS_CONF = "mapred.redishashinputformat.hosts";
	public static final String REDIS_DATE_KEY_CONF = "mapred.redishashinputformat.date";

	//IP 등록
	public static void setRedisHosts(Job job, String hosts) {
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
	}
	
	//날짜 등록
	public static void setRedisDateKey(Job job, String Date) {
		job.getConfiguration().set(REDIS_DATE_KEY_CONF, Date);
	}

	//This method will return a list of InputSplit objects.  The framework uses this to create an equivalent number of map tasks
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		//Get our configuration values and ensure they are set
		String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		if (hosts == null || hosts.isEmpty()) {
			throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
		}

//		String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
//		if (hashKey == null || hashKey.isEmpty()) {
//			throw new IOException(REDIS_HASH_KEY_CONF + " is not set in configuration.");
//		}
		
		String Date = job.getConfiguration().get(REDIS_DATE_KEY_CONF);
		if (Date == null || Date.isEmpty()) {
			throw new IOException(REDIS_DATE_KEY_CONF + " is not set in configuration.");
		}

		//Create an input split for each Redis instance
		//More on this custom split later, just know that one is created per host
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (String host : hosts.split(",")) {
			splits.add(new RedisHashInputSplit(host, Date));
		}

		return splits;
	}

	//This method creates an instance of our RedisHashRecordReader
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new RedisHashRecordReader();
	}

	//This custom RecordReader will pull in all key/value pairs from a Redis instance for a given hash
	public static class RedisHashRecordReader extends RecordReader<Text, Text> {

		//A number of member variables to iterate and store key/value pairs from Redis
		private Iterator<Entry<String, String>> keyValueMapIter = null;
		private Text key = new Text(), value = new Text();
		private float processedKVs = 0, totalKVs = 0;
		private Entry<String, String> currentEntry = null;

		//Initialize is called by the framework and given an InputSplit to process
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			//Get the host location from the InputSplit
			String host = split.getLocations()[0];
			String date = ((RedisHashInputSplit) split).getDate();

			//Create a new connection to Redis
			//@SuppressWarnings("resource")
			Jedis jedis = new Jedis(host);
			jedis.connect();
			jedis.getClient().setTimeoutInfinite();

			//Get all the key/value pairs from the Redis instance and store them in memory
			
			Integer count = 0;

			//date에 해당하는 모든 페이지들에 대한 정보를 가져온다
			Map<String, String> inputs =		makeinputs(jedis, date, count);

			totalKVs = count;
			keyValueMapIter = inputs.entrySet().iterator();
			System.out.println("Got " + totalKVs + " from " + date); jedis.disconnect();
		}
		
		public Map<String, String> makeinputs(Jedis jedis, String date, Integer count){
			
			//그 날짜의 모든 데이터를 겟, DB 0
			
			jedis.select(0);

			Set<String> allpages = jedis.keys(date+"*");
			Map<String, String> touchs ;
			
			
			//그 날짜의 모든 데이터를 겟, DB3
			jedis.select(3);
//			Set<String> allpages = jedis.keys(date+"*");
//			Map<String, String> touchs ;
			List<String> gestures;
			String converted, touchhash, gesturename,tmp;
			char gesturetype = 0;
			Map<String, String> inputs = new HashMap<String, String>();
			
			//각각의 페이지에 대하여, DB3
			for(String pagehash : allpages){

				converted = new String();
				jedis.select(3);
				gestures = jedis.hvals(pagehash);
				converted += gestures.get(0);//url
				converted += ',';
				converted += gestures.get(1);//기기명
				converted += ',';
				converted += gestures.get(2);//국가명

				//각 페이지 내부 - 제스쳐에 대하여, DB0
				for(int i = 3; i < gestures.size() ; i ++ ){
					
					jedis.select(0);
					//페이지 안의 제스쳐들의 정보를 겟
					touchs = jedis.hgetAll(gestures.get(i));

					gesturename = touchs.get("gesture");

					//제스쳐 타입에 따른 인식번호
					if("tab".equals(gesturename)){
						gesturetype = '1';
					}
					else if("doubletab".equals(gesturename)){
						gesturetype = '2';
					}
					else if("longtab".equals(gesturename)){
						gesturetype = '3';
					}
					else if("drag".equals(gesturename)){
						gesturetype = '4';
					}
					else if("pinch".equals(gesturename)){
						gesturetype = '5';
					}
					else if("spread".equals(gesturename)){
						gesturetype = '6';
					}
					else if("copy".equals(gesturename)){
						gesturetype = '7';
					}
					

					//터치데이터 디비
					jedis.select(1);
					
					//start 데이터
					touchhash = touchs.get("start");
					tmp = convertTouchData(jedis, gesturetype, '1', touchhash);
					converted += tmp;
					
					if(gesturetype == '4'){//드래그 일때만 무브가 있다
						List<String> moves;
						
						touchhash = touchs.get("move");
						moves = jedis.hvals(touchhash);
						
						//move 데이터
						for(String movehash : moves){
							tmp = convertTouchData(jedis, gesturetype, '3', movehash);
							converted += tmp;
						}
						
					}
					
					//end 데이터
					touchhash = touchs.get("end");
					tmp = convertTouchData(jedis, gesturetype, '2', touchhash);
					converted += tmp;
					
					
					
					
				}//모든 제스쳐(터치데이터를)를 다 한 줄(converted)로 넣었다면
				
				//map input에 추가
				inputs.put("input "+ (count++), converted);


			}
			
			return inputs;
		}
		
		//터치데이터를 일렬로 컨버팅
		public String convertTouchData(Jedis jedis, char gesturetype, char touchtype, String hashkey){
			String touchData = new String();
			String tmp;

			
			touchData += ',';
			touchData += gesturetype;
			touchData += '1';
			
			//시간 불러오기
			tmp = jedis.hget(hashkey, "timestamp");
			touchData += tmp;
			
			//클라이언트X 불러오기
			tmp = jedis.hget(hashkey, "clientX");
			touchData += tmp;
			
			//클라이언트Y 불러오기
			tmp = jedis.hget(hashkey, "clientY");
			touchData += tmp;
			
			//페이지X 불러오기
			tmp = jedis.hget(hashkey, "pageX");
			touchData += tmp;
			
			//페이지Y 불러오기
			tmp = jedis.hget(hashkey, "pageY");
			touchData += tmp;
			
			//눌러진 태그 불러오기
			tmp = jedis.hget(hashkey, "tag");
			touchData += tmp;
			
			
			return touchData;
		}

		//This method is called by Mapper’s run method to ensure all key/value pairs are read
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (keyValueMapIter.hasNext()) {
				//Get the current entry and set the Text objects to the entry
				currentEntry = keyValueMapIter.next();
				key.set(currentEntry.getKey());
				value.set(currentEntry.getValue());
				return true;
			} else {
				return false;
			}
		}

		//The next two methods are to return the current key/value pairs.  Best practice is to re-use objects rather than create new ones, i.e. don’t use “new”
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		//This method is used to report the progress metric back to the framework.  It is not required to have a true implementation, but it is recommended.
		public float getProgress() throws IOException, InterruptedException {
			return processedKVs / totalKVs;
		}

		public void close() throws IOException {
			/* nothing to do */
		}

	} // end RedisHashRecordReader

	public static class RedisHashInputSplit extends InputSplit implements Writable {

		// Two member variables, the hostname and the hash key (table name)
		private String location = null;
		private String date = null;

		public RedisHashInputSplit() {
			// Default constructor required for reflection
		}

		public RedisHashInputSplit(String redisHost, String date) {
			this.location = redisHost;
			this.date = date;
		}

		public String getDate() {
			return this.date;
		}

		// The following two methods are used to serialize the input information for an individual task
		public void readFields(DataInput in) throws IOException {
			this.location = in.readUTF();
			this.date = in.readUTF();
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(location);
			out.writeUTF(date);
		}

		// This gets the size of the split so the framework can sort them by size.  This isn’t that important here, but we could query a Redis instance and get the bytes if we desired
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		// This method returns hints to the framework of where to launch a task for data locality
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] { location };
		}

	} // end RedisHashInputSplit

} // end RedisHashInputFormat