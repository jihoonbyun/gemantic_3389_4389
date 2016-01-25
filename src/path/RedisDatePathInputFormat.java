package path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import redis.clients.jedis.Jedis;

//This input format will read all the data from a given set of Redis hosts
public class RedisDatePathInputFormat extends InputFormat<Text, ObjectWritable> {


	
	//Again, the CSV list of hosts and a hash key variables and methods for configuration
	public static final String REDIS_HOSTS_CONF = "mapred.redisdatepathinputformat.hosts";
	public static final String REDIS_DATE_KEY_CONF = "mapred.redisdatepathinputformat.key";
	
	public static void setRedisHosts(Job job, String hosts) {
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
		System.out.println("	-	input path : "+ hosts);
	}
	
	
	//��Ÿ����ּҴ� �ƿ�ǲ �н����� �ʿ�������, ���⼭ �ӽ÷� �ʿ������� �ϴ� �������� ����
	public static String metahost;
	public static void RedisMetaHost(String hosts){
		metahost = hosts;
	}
	

	public static void setRedisDateKey(Job job, String hashKey) {
		job.getConfiguration().set(REDIS_DATE_KEY_CONF, hashKey);
		System.out.println("	-	input hash : "+ hashKey);
	}

	//This method will return a list of InputSplit objects.  The framework uses this to create an equivalent number of map tasks
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		//Get our configuration values and ensure they are set
		String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		if (hosts == null || hosts.isEmpty()) {
			throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
		}
		


		String hashKey = job.getConfiguration().get(REDIS_DATE_KEY_CONF);
		if (hashKey == null || hashKey.isEmpty()) {
			throw new IOException(REDIS_DATE_KEY_CONF + " is not set in configuration.");
		}

		//Create an input split for each Redis instance
		//More on this custom split later, just know that one is created per host
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (String host : hosts.split(",")) {
			splits.add(new RedisHashInputSplit(host, hashKey));
		}

		return splits;
	}

	//RecordReader �Լ��� �ϳ��� ���ڵ徿 map()�޼ҵ�� �����ϴ� ������ �Ѵ�.
	//This method creates an instance of our RedisHashRecordReader
	public RecordReader<Text, ObjectWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new RedisHashRecordReader();
	}

	//This custom RecordReader will pull in all key/value pairs from a Redis instance for a given hash
	public static class RedisHashRecordReader extends RecordReader<Text, ObjectWritable> {

		//A number of member variables to iterate and store key/value pairs from Redis
		private Iterator<Entry<String, PathData>> keyValueMapIter = null;
		private Text key = new Text();
		private ObjectWritable value = new ObjectWritable();
		private float processedKVs = 0, totalKVs = 0;
		private Entry<String, PathData> currentEntry = null;

		//Initialize is called by the framework and given an InputSplit to process
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			//Get the host location from the InputSplit
			String host = split.getLocations()[0];
			String hashKey = ((RedisHashInputSplit) split).getHashKey();

			//Create a new connection to Redis
			@SuppressWarnings("resource")
			Jedis jedis = new Jedis(host,3389);
			jedis.connect();
			
			

			@SuppressWarnings("resource")

			Jedis metajedis = new Jedis(metahost, 4389);
			metajedis.connect();
			metajedis.select(5);
			
			
			jedis.select(7);
			
			//PathData input;

			Map<String,PathData> inpt = new HashMap<String, PathData>();
			//Map<String,Integer> count = new HashMap<String, Integer>();
			
			//System.out.println("keys = " + ("*:"+hashKey+":*"));
			
			Set<String> hashkeys = jedis.keys("*:"+hashKey+":*");
			jedis.getClient().setTimeoutInfinite();
			//	String[] keys;
			//int num;
			
			String tmpstr;
			String[] url_list,mukem_list, url_key;
			ArrayList<String> tagnumber_list = new ArrayList<String>();
			String[] destination_list = null, timehost_list = null, timepages_list = null, events_list = null;

			PathData newpath;
			Link_Data newlink;
			int mukem_cnt, dest_cnt = -1, timehost_cnt = -1, timepages_cnt = -1, event_cnt = -1;
			

			//������ �н����� �����͸� �����´�
			for(String hash : hashkeys){
				//System.out.println("hashkey = "+hash);
				newpath = new PathData();
				
				tmpstr = jedis.hget(hash, "pathurl");
				//System.out.println("url_list = "+tmpstr);
				if("null".equals(tmpstr)){
					System.out.println("�н� �̵� ��ΰ� �����ϴ�(db7)");
					//System.out.println("~~~~~~~~~!!!!!!!!!!!! Del key : " + hash);
					jedis.del(hash);
					//System.out.println("~!@$%^%$@!#%^&#@~!#%^&%@!~@$^&@~!%&*mang!");
					continue;
				}
				
				url_list = tmpstr.split(",");
				tmpstr = jedis.hget(hash, "path");
				//System.out.println("tagnumber_list = "+tmpstr);
				for(int t =0; t < tmpstr.split(",").length; t++){
					tagnumber_list.add(tmpstr.split(",")[t]);
				}
				//������ ���� �������� �翬�� ��ũ�ε���������....���Ƿ� -1�� �ִ´�
				tagnumber_list.add("-1");
				tmpstr = jedis.hget(hash, "mukem");
				//System.out.println("mukem_list = "+tmpstr);
				mukem_list = tmpstr.split("\\|");
				System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~muken len = " + mukem_list.length);
				mukem_cnt=0;
				
				tmpstr = jedis.hget(hash, "destination");
				System.out.println("destination_list = "+tmpstr);
				dest_cnt = -1;
				if(tmpstr != null){//��ǥ�� ������
					destination_list = tmpstr.split("\\|");
					dest_cnt = 0;					
				}
//
				tmpstr = jedis.hget(hash, "timepages");
				System.out.println("timepage_list = "+tmpstr);
				timepages_cnt = -1;
				if(tmpstr != null){//��ǥ�� ������
					timepages_list = tmpstr.split("\\|");
					timepages_cnt = 0;					
				}
//
				tmpstr = jedis.hget(hash, "timehost");
				System.out.println("timehost_list = "+tmpstr);
				timehost_cnt = -1;
				if(tmpstr != null){//��ǥ�� ������
					timehost_list = tmpstr.split("\\|");
					timehost_cnt = 0;					
				}
				
				
				tmpstr = jedis.hget(hash, "events");
				System.out.println("events_list = "+tmpstr);
				event_cnt = -1;
				//System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~eventkey: " +hash);
				if(tmpstr != null){//��ǥ�� ������
					//System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~event : " + tmpstr);
					events_list = tmpstr.split("\\|");
					event_cnt = 0;					
				}
				
//				System.out.println("tagnumber_list = "+hash);
//				System.out.println("mukem_list = "+hash);
				
				String[] convergence_key;
				
				newpath.linkdata = new Link_Data[url_list.length];
				
				//�ϳ��� �� �����͸� ������ ������ ��ũ������ �迭�� �����
				for(int i = 0 ; i < url_list.length ; i++){
					newlink = new Link_Data();
					newlink.link_name = url_list[i] + "," + tagnumber_list.get(i); //�±��� �̸�. url+�±׹�ȣ
					System.out.println("pages::: " + newlink.link_name);
					newlink.touchdata_key = "";
		
					while(mukem_cnt < mukem_list.length){
						
						
					//	System.out.println(" num = " + mukem_cnt);
					//	System.out.println("mukenkey = " + mukem_list[mukem_cnt]);
						url_key = mukem_list[mukem_cnt].split(",");
						
						//System.out.println("-----------------------------urlkey = " + mukem_list[mukem_cnt]);
						
						System.out.println("-------------------------" + url_key[0] + "xxx" + url_key[1] + "mmm" + url_list[i]);
						if(url_key[0].equals(url_list[i])){//���� �ֱ�
							
							//System.out.println("�̰��� �����̴�@#@#@" + " ���˿� : " +  url_list[i] +"����Ű :" +  mukem_list[mukem_cnt]);
							
							if( i != url_list.length-1){
								if(url_list[i].equals(url_list[i+1]) && !newlink.touchdata_key.equals("")){
									break;								
								}
							}
							
							
							
							// ����Ƽ���̼� destination_list ���� 
							//["19699,fca17fd2-dc53-4c2b-b6a4-745829a690c9,�ð�","30558,860b293d-cb9b-4223-b864-40ab6579fbc1,�ð�", "7187,96dbcb21-01b5-417a-b81c-b2d7edab7e21,�ð�"]
							//19699 : �������������� �ٸ� �������� �̵�����(a�±�Ŭ��)�ɸ��ð�
							//fca17fd2-dc53-4c2b-b6a4-745829a690c9 : ����Ű
							
							if(dest_cnt != -1){//��ǥ�� ���� ��쿡�� ����
								if(dest_cnt < destination_list.length){//������ ����� �ʾ��� ���� ����
									convergence_key = destination_list[dest_cnt].split(",");
									//System.out.println("key in conv = " + url_key[1] + " dest = " + convergence_key[1] + " time = " + convergence_key[0]);
									if(url_key[1].equals(convergence_key[1])){
										
										//�̰��� ù���������� ������������ �̵��Ҷ����� �ɸ��ð��̴�.
										newlink.destination_average_time += Integer.parseInt(convergence_key[0]);
										newlink.destination_totalcount++;
										dest_cnt++;
									}
								}
								if(dest_cnt == destination_list.length) { newlink.destination_average_time = newlink.destination_average_time/dest_cnt;}
							}
							
							if(timehost_cnt != -1){//�ð��� ���� ��쿡�� ����
								if(timehost_cnt < timehost_list.length){//������ ����� �ʾ��� ���� ����
									convergence_key = timehost_list[timehost_cnt].split(",");
								//	System.out.println("key in conv = " + url_key[1] + " timehost = " + convergence_key[0]);
									if(url_key[1].equals(convergence_key[0])){
										//�����ѽð����̴�
										newlink.timehost_average_time += Long.parseLong(convergence_key[1]);
										newlink.timehost_totalcount++;
										timehost_cnt++;
									}
								}
								if(timehost_cnt == timehost_list.length) { newlink.timehost_average_time = newlink.timehost_average_time/timehost_cnt;}
							}
							
							
							
							if(timepages_cnt != -1){//�ð��� ���� ��쿡�� ����
								if(timepages_cnt < timepages_list.length){//������ ����� �ʾ��� ���� ����
									
									convergence_key = timepages_list[timepages_cnt].split(",");
									System.out.println("����� Ÿ����������" + url_key[1] + "----" + convergence_key[1]);
									if(url_key[1].equals(convergence_key[1])){
										System.out.println("key in conv = " + url_key[1] + " timepages!!! = " + convergence_key[0]);
										newlink.timepage_average_time +=  Long.parseLong(convergence_key[2]);
										newlink.timepage_totalcount++;
										timepages_cnt++;
									}
								}
								if(timepages_cnt == timepages_list.length) { newlink.timepage_average_time = newlink.timepage_average_time/timepages_cnt;}
							}
							
							
							
							
							//�̺�Ʈ�ǰ�� ���������� �������� �̺�Ʈ�� ��ϵɼ��ִ�.(���̺�Ʈ�� �޼���Ƚ�� x �̺�Ʈ����)
							//a.url fn n,click,9f86bf62-2455-4008-802f-e975a10f51b5,9901,1427891038459
							if(event_cnt != -1){
								
								//�ش��������� �ش��ϴ� �̺�Ʈ�� �ϳ��� ī��Ʈ�ϸ鼭, �ٸ� �������� ��ȯ�ɶ� �극��ũ
								while(true){
										if(event_cnt < events_list.length){
											if(url_key[1].equals(events_list[event_cnt].split(",")[2])){
												newlink.events_totalcount++;
												newlink.events_average_time += Long.parseLong(events_list[event_cnt].split(",")[3]);
												event_cnt++;
											}
											else{
												break;
											}
										}
										else if(event_cnt == events_list.length) {newlink.events_average_time = newlink.events_average_time/event_cnt; break;}
										
										else { break;}
										
								}
							}							
						
							/*
							if(event_cnt != -1){//��ǥ�� ���� ��쿡�� ����
								if(event_cnt < events_list.length){//������ ����� �ʾ��� ���� ����
									//System.out.println("@@@@@@@@@@@@@@@@@@eventlist[" + event_cnt + "] = " + events_list[event_cnt]);
									convergence_key = events_list[event_cnt].split(",");
								//	System.out.println("key in conv = " + url_key[1] + " events = " + convergence_key[0]);
									if(url_key[1].equals(convergence_key[2])){
										newlink.events = Long.parseLong(convergence_key[3]);
										event_cnt++;
									}
								}
							}
							*/
				
							
							newlink.touchdata_key += (mukem_list[mukem_cnt++] + "|");
							
						}
						else
					
							break;
						
					}
					//System.out.println("touchdatakey = " + newlink.touchdata_key);
					
					if(newlink.touchdata_key.length() != 0){
						newlink.touchdata_key = newlink.touchdata_key.substring(0, newlink.touchdata_key.length()-1);
						
					}

					
					
					
					//System.out.println("touchkey for " + newlink.link_name + " is = " + newlink.touchdata_key);
					
					//newlink.touchdata_key = mukem_list[i];
					newpath.linkdata[i] = newlink;
				}
				
				System.out.println("�̰� ����Ƽ���̼� ī��Ʈ!!!!" + newpath.linkdata[0].destination_totalcount);
				System.out.println("�̰� Ÿ��ȣ��Ʈ ī��Ʈ!!!!" + newpath.linkdata[0].timehost_totalcount);
				System.out.println("�̰� Ÿ�������� ī��Ʈ!!!!" + newpath.linkdata[0].timepage_totalcount);
				System.out.println("�̰� �̺�Ʈ ī��Ʈ!!!!" + newpath.linkdata[0].events_totalcount);
				
				
				//����Ʈ url�� Ű������ �Ͽ� �ʿ� ����
				tmpstr = jedis.hget(hash, "url");
				newpath.site_url = tmpstr;
				newpath.date = hash.split(":")[1];
				newpath.userid = jedis.hget(hash, "guid");
				System.out.println("~~~~~~~~~~~~id : " + newpath.userid);
				
				
				inpt.put(hash, newpath);
				
				String metasplit[] = hash.split(":");
				
				String metakey = metasplit[0] + ":" + metasplit[1];
				metajedis.sadd(metakey + ":pureuser", newpath.userid);//���湮��
				metajedis.rpush(metakey + ":alluser", newpath.userid);//�ѹ湮��
				
				String metaallkey = metasplit[0];
				//ù �湮
				if(metajedis.sismember(metaallkey + ":users", newpath.userid) == false){

					metajedis.sadd(metaallkey + ":users", newpath.userid);
					metajedis.sadd(metakey + ":firstuser", newpath.userid);//ù�湮��
	
				}								
			}
			
			System.out.println("finish-input");

			//Get all the key/value pairs from the Redis instance and store them in memory
			totalKVs = hashkeys.size();
			keyValueMapIter = inpt.entrySet().iterator();
			
			
		System.out.println("Got " + totalKVs + " from " + hashKey); jedis.disconnect();
		}

		//This method is called by Mapper��s run method to ensure all key/value pairs are read
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

		//The next two methods are to return the current key/value pairs.  Best practice is to re-use objects rather than create new ones, i.e. don��t use ��new��
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		public ObjectWritable getCurrentValue() throws IOException, InterruptedException {
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
		private String hashKey = null;

		public RedisHashInputSplit() {
			// Default constructor required for reflection
		}

		public RedisHashInputSplit(String redisHost, String hash) {
			this.location = redisHost;
			this.hashKey = hash;
		}

		public String getHashKey() {
			return this.hashKey;
		}

		// The following two methods are used to serialize the input information for an individual task
		public void readFields(DataInput in) throws IOException {
			this.location = in.readUTF();
			this.hashKey = in.readUTF();
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(location);
			out.writeUTF(hashKey);
		}

		// This gets the size of the split so the framework can sort them by size.  This isn��t that important here, but we could query a Redis instance and get the bytes if we desired
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		// This method returns hints to the framework of where to launch a task for data locality
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] { location };
		}

	} // end RedisHashInputSplit

} // end RedisHashInputFormat