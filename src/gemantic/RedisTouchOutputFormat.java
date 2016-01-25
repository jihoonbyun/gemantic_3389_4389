package gemantic;

import java.awt.Point;
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
public class RedisTouchOutputFormat extends OutputFormat<Text, ObjectWritable> {

	//These static conf variables and methods are used to modify the job configuration.  This is a common pattern for MapReduce related classes to avoid the magic string problem
	public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
	static String DBhost;
	public static final String REDIS_HOSTS_CONF_DB = "mapred.redishashoutputformat.hosts_DB";
//	public static final String REDIS_HASH_KEY_CONF = "mapred.redishashoutputformat.key";

	public static void setRedisHosts(Job job, String hosts) {
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
		System.out.println("	-	output path : "+ hosts);
	}

	public static void setRedisHosts_DB(Job job, String hosts) {
		DBhost = hosts;
		job.getConfiguration().set(REDIS_HOSTS_CONF_DB, hosts);
		System.out.println("	-	DB path : "+ hosts);
	}

//	public static void setRedisHashKey(Job job, String hashKey) {
//		job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
//		System.out.println("	-	output hash : "+ hashKey);
//	}

	//This method returns an instance of a RecordWriter for the task.  Note how we are pulling the variables set by the static methods during configuration
	public RecordWriter<Text, ObjectWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
//		String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
		String csvHosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		return new RedisHashRecordWriter(csvHosts);
	}

	//This method is used on the front-end prior to job submission to ensure everything is configured correctly
	public void checkOutputSpecs(JobContext job) throws IOException {
		String hosts = job.getConfiguration().get(REDIS_HOSTS_CONF);
		if (hosts == null || hosts.isEmpty()) {
			throw new IOException(REDIS_HOSTS_CONF + " is not set in configuration.");
		}

//		String hashKey = job.getConfiguration().get(REDIS_HASH_KEY_CONF);
//		if (hashKey == null || hashKey.isEmpty()) {
//			throw new IOException(REDIS_HASH_KEY_CONF + " is not set in configuration.");
//		}
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
	//	private HashMap<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();
		String host;
		Jedis jedis;
		
		// This is the name of the Redis hash
	//	private String hashKey = null;

		public RedisHashRecordWriter(String hosts) {
		//	this.hashKey = hashKey;
			this.host = hosts;
			// Create a connection to Redis for each host
			// Map an integer 0-(numRedisInstances - 1) to the instance
//			int i=0;
//			for (String host : hosts.split(",")) {
//				Jedis jedis = new Jedis(host);
//				jedis.connect();
//				jedisMap.put(i++, jedis);
//			}
		}

		public void setMetaDatas(String key, String date, String country, int visit, int utype){
			
			String usertype =null;
			if(utype == 1){
				usertype = "bounce";
			}
			else if(utype == 2){
				usertype = "close";
			}
			else if(utype == 3){
				usertype = "complete";
			}
			else if(utype == 4){
				usertype ="destination";
			}
			else if(utype == 5){
				usertype = "timehost";
			}
			else if(utype == 6){
				usertype = "events";
			}
			else if(utype == 7){
				usertype = "timepage";
			}
			else{
				usertype="all";
			}
			
			String url = key.split(",")[0];
			String device = key.split(",")[1];
			
			String hashkey = url + "," + date + ",meta";
			String result;
			
			jedis.select(4);
			
			//사용자 수 체크
			int visitor = visit;
				
			result = jedis.hget(hashkey, "visitor");
			if(result != null){
				visitor += Integer.parseInt(result);
			}
			
			jedis.hset(hashkey, "visitor", ""+visitor);
			
			String devicelist = null;
			
			//기기목록
			result = jedis.hget(hashkey, "devices");
			if(result == null){
				devicelist = device;
			}
			else{
				String[] delist = result.split(",");
				int a=1;
				for(String st : delist){
					if(device.equals(st)){
						a=0;
						break;
					}
				}
				
				//중복 없으면
				if(a == 1){
					devicelist = result + "," + device;
				}	
				else
					devicelist = result;
			}
			
			jedis.hset(hashkey, "devices", devicelist);
			
			//국가목록
			String countrylist = null;
			
			result = jedis.hget(hashkey, "countrys");
			if(result == null){
				countrylist = country;
			}
			else{
				String[] colist = result.split(",");
				int a=1;
				for(String st : colist){
					if(country.equals(st)){
						a=0;
						break;
					}
				}
				
				//중복 없으면
				if(a == 1){
					countrylist = result + "," + country;
				}
				else
					countrylist = result;
			}
			
			jedis.hset(hashkey, "countrys", countrylist);
			
			
			
			//유저타입목록
			
			String typehashkey = url + "," + date + ",types";
			String thistype = null;
			String typevalue = device + "," + country;
			result = jedis.hget(typehashkey, typevalue);
			if(result == null){
				thistype = usertype;
			}
			else{
				String[] tlist = result.split(",");
				int t=1;
				for(String type : tlist){
					if(usertype.equals(type)){
						t=0;
						break;
					}
				}
				
				//중복 없으면
				if(t == 1){
					thistype = result + "," + usertype;
				}	
				else
					thistype = result;
			}
			
			jedis.hset(typehashkey, typevalue, thistype);			
			
			
			
			
			
			//기기별횟수

			hashkey = url + "," + date + ",device";
			
			result = jedis.hget(hashkey, device);
			int visit_dev = visit;
			if(result != null)
				visit_dev += Integer.parseInt(result);
			
			jedis.hset(hashkey, device, ""+visit_dev);
			
			//국가별횟수

			hashkey = url + "," + date + ",country";
			
			result = jedis.hget(hashkey, country);
			int visit_con = visit;
			if(result != null)
				visit_con += Integer.parseInt(result);
			
			jedis.hset(hashkey, country, ""+visit_con);
			
			
		}

		public String MakeUrl(String input){
			
			  String url = input.split(",")[0];
			  String device = input.substring(url.length()+1);
			  String key_hash;
				  
			  String[] urls = url.split("/");
			  key_hash = urls[0] + ":/";
				
			  for(int i=1;i<urls.length;i++){
				key_hash += urls[i];
				key_hash += "/";	
			  }
			  key_hash += ":";
			  key_hash += device;
			  
			return key_hash;
			
		}

		public String MakeOnlyUrl(String input){
			
			  String url = input.split(",")[0];
			  String key_hash;
				  
			  String[] urls = url.split("/");
			  key_hash = urls[0] + "/";
				
			  for(int i=1;i<urls.length;i++){
				key_hash += urls[i];
				key_hash += "/";	
			  }
			 // key_hash += ":";
			 // key_hash += device;
			  
			return key_hash;
			
		}
		
		// The write method is what will actually write the key value pairs out to Redis

		public void write(Text key, ObjectWritable value) throws IOException, InterruptedException {
			
			/*
			 * (non-Javadoc)
			 * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
			 * 
			 * 
			 * 주석작성자(변지훈 2015.4.13)
			 * 
			 * Reduce에서 내보낸 키/밸류쌍을 과거 DB데이터와 합산하여 저장한다
			 * 
			 * 키 : (스트링) url + device + 컨버전스명
			 * 밸류 :(오브젝트) {bounce : "데이터", close :"데이터", ...destination : "데이터"..}
			 * 
			 */			
			
			
			System.out.println("--------Reduce Write start");
			jedis = new Jedis(host, 4389);
			@SuppressWarnings("resource")
			Jedis Dbjedis = new Jedis(DBhost, 3389);
			ResultData result;
			String key_date, key_country, key_type;
			String key_area_time, key_area_flow, key_area_spread, key_area_reverse,key_area_finish,key_area_fast, key_area_slow;
			String key_tag_time, key_tag_copy, key_tag_reverse;
			String key_fault_tab, key_heat_map, key_read_pattern;
			String key_pure_user, key_all_user;
			
			//제대로 된 결과일 때만 처리
			if(value.getDeclaredClass() == ResultData.class){
				
				jedis.connect();
			
				
				//과거 데이터를 불러온 뒤 합친다
				//레디스 원디비가 여러개고, 데이터가 분산되어있을때 합산해서 계산한다는 의도이다.
				//주의 :: 같은 원디비데이터를 여러번 실행시키면안된다(중복되기 때문)
				result = ResultData.loadmeta((ResultData) value.get(), jedis) ;	
				
				//DB에 시간 데이터를 넣기 위한 것.
				String dbkey = MakeUrl(key.toString()) + ":summary";
				
				//원디비 6번
				Dbjedis.select(6);
				Dbjedis.hset(dbkey,"average_time",""+((double)(result.all_area_time)));
				

				setMetaDatas(key.toString(), ""+result.get_day(), result.get_country(), ((ResultData) value.get()).user_cnt, result.type);

				
				
				//페이지,디바이스-날짜
				jedis.select(0);//임시, 0 으로 바꿀예정
				key_date = key.toString()+","+result.get_day();
				jedis.hset(key.toString(),((Integer)result.get_day()).toString() , key_date);
				
				//날짜-국가
				jedis.select(1);
				key_country = key_date+"," + result.get_country();
				jedis.hset(key_date, result.get_country() , key_country);
				
				
				
				
				
				key_type = key_country;
				if(result.type == 1){
					key_type += ",bounce";
				}
				else if(result.type == 2){
					key_type += ",close";
				}
				else if(result.type == 3){
					key_type += ",complete";
				}
				else if(result.type == 4){
					key_type += ",destination";
				}
				else if(result.type == 5){
					key_type += ",timehost";
				}
				else if(result.type == 6){
					key_type += ",events";
				}
				else if(result.type == 7){
					key_type += ",timepage";
				}
				
			//	System.out.println("key = "+key_type);
				
				//키 생성(배열형 데이터)
				key_area_time = key_type + "," + "area_time";
				key_area_flow = key_type + "," + "area_flow";
				key_area_spread = key_type + "," + "area_spread";
				key_area_reverse = key_type + "," + "area_reverse";
				key_area_finish = key_type + "," + "area_finish";
				key_area_fast = key_type + "," + "area_fast";
				key_area_slow = key_type + "," + "area_slow";
				key_tag_time = key_type + "," + "tag_time";
				key_tag_copy = key_type + "," + "tag_copy";
				key_tag_reverse = key_type + "," + "tag_reverse";
				key_fault_tab = key_type + "," + "fault_tab";
				key_heat_map = key_type + "," + "heat_map";
				key_read_pattern = key_type + "," + "read_pattern";
				key_pure_user = key_type + "," + "pure_user";
				key_all_user = key_type + "," + "all_user";
				
				// /**/ 는 배열(list) 형 데이터이다 
				//국가-데이터
				jedis.select(2);
				HashMap<String, String> inputdata = new HashMap<>();
				inputdata.put("time_sum" , ((Integer)result.get_time_sum()).toString());
				inputdata.put("area_speed" , ((Double)result.get_area_speed()).toString());
				inputdata.put("all_area_time" , ""+result.all_area_time);
				/**/inputdata.put("area_time" , key_area_time);
				/**/inputdata.put("area_flow" , key_area_flow);
				/**/inputdata.put("area_spread" , key_area_spread);
				/**/inputdata.put("area_reverse" , key_area_reverse);
				/**/inputdata.put("area_finish" , key_area_finish);
				/**/inputdata.put("area_fast" , key_area_fast);
				/**/inputdata.put("area_slow" , key_area_slow);
				/**/inputdata.put("tag_time" , key_tag_time);
				/**/inputdata.put("tag_copy" , key_tag_copy);
				/**/inputdata.put("tag_reverse" , key_tag_reverse);
				inputdata.put("drag_left" , ((Integer)result.get_drag_loc()[0]).toString());
				inputdata.put("drag_right" , ((Integer)result.get_drag_loc()[1]).toString());
				/**/inputdata.put("fault_tab" , key_fault_tab);
				/**/inputdata.put("heat_map" , key_heat_map);
				/**/inputdata.put("read_pattern" , key_read_pattern);
				/**/inputdata.put("pure_user" , key_pure_user);
				/**/inputdata.put("all_user" , key_all_user);
				
				inputdata.put("tab" , ((Integer)result.get_motion_count()[0]).toString());
				inputdata.put("doubletab" , ((Integer)result.get_motion_count()[1]).toString());
				inputdata.put("longtab" , ((Integer)result.get_motion_count()[2]).toString());
				inputdata.put("drag" , ((Integer)result.get_motion_count()[3]).toString());
				inputdata.put("pinch" , ((Integer)result.get_motion_count()[4]).toString());
				inputdata.put("spread" , ((Integer)result.get_motion_count()[5]).toString());				
				inputdata.put("copy" , ((Integer)result.get_motion_count()[6]).toString());
		
				inputdata.put("complete" , ((Double)result.get_complete()).toString());
				inputdata.put("complete_time" , ((Long)result.get_complete_time()).toString());
				inputdata.put("destination_time" , ((Long)result.get_destination_time()).toString());
				inputdata.put("timehost_time" , ((Long)result.get_timehost_time()).toString());
				inputdata.put("timepage_time" , ((Long)result.get_timepage_time()).toString());
				inputdata.put("events_time" , ((Long)result.get_events_time()).toString());

				inputdata.put("destination_totalcount" , ((Integer)result.get_destination_totalcount()).toString());
				inputdata.put("timehost_totalcount" , ((Integer)result.get_timehost_totalcount()).toString());
				inputdata.put("timepage_totalcount" , ((Integer)result.get_timepage_totalcount()).toString());
				inputdata.put("events_totalcount" , ((Integer)result.get_events_totalcount()).toString());
					
				
				inputdata.put("user_cnt" , ((Integer)result.user_cnt).toString());
				inputdata.put("left_user" , ((Integer)result.left_user).toString());
				inputdata.put("right_user" , ((Integer)result.right_user).toString());
				inputdata.put("both_user" , ((Integer)result.both_user).toString());
				

				System.out.println("리듀스아웃풋[주소+디바이스+국가+컨버전스명]  " + key_type);
				System.out.println("리듀스아웃풋[데스티네이션]  " + result.destination_totalcount);
				System.out.println("리듀스아웃풋[호스트]  " + result.timehost_totalcount);
				System.out.println("리듀스아웃풋[페이지]  " + result.timepage_totalcount);
				System.out.println("리듀스아웃풋[이벤트]  " + result.events_totalcount);

				
				//레디스에 넣기
				jedis.hmset(key_type, inputdata);

				
				
				//세부 데이터 넣기
				jedis.select(3);
				
				pushdouble(key_area_time, result.get_area_rate(), jedis);
				pushint(key_area_flow, result.get_area_flow(), jedis);

			//	System.out.println("flow size = " + result.get_area_flow().length);
				pushint(key_area_spread, result.get_area_spread(), jedis);
				pushint(key_area_reverse, result.get_area_reverse(), jedis);
				pushint(key_area_finish, result.get_area_finish(), jedis);
				pushint(key_area_fast, result.get_area_fast(), jedis);
				pushint(key_area_slow, result.get_area_slow(), jedis);
				pushdouble(key_tag_time, result.get_tag_rate(), jedis);
				pushint(key_tag_copy, result.get_tag_copy(), jedis);
				pushint(key_tag_reverse, result.get_tag_reverse(), jedis);
				pushint(key_area_flow, result.get_area_flow(), jedis);
				pushint(key_read_pattern, result.get_read_pattern(), jedis);
				pushpoint(key_fault_tab, result.get_fault_tab(), jedis);
				pushStringList(key_all_user, result.get_all_user(), jedis);
				pushStringSet(key_pure_user, result.get_pure_user(), jedis);
				pushpoint(key_heat_map, result.get_heat_map(), jedis);

				jedis.select(6);
				String sumkey = MakeOnlyUrl(key.toString()) + ","+result.get_day();
				
				jedis.hset(sumkey, "user_cnt", ((Integer)result.user_cnt).toString());
				
			}
			else{
				System.out.println("잘못된 클래스가 들어왔습니다 - write");
				return;
			}

		}
		
		public void pushint(String key, int[] data, Jedis jedis){
			jedis.del(key);
			for(Integer i : data )
			jedis.rpush(key, i.toString());
		}
		public void pushdouble(String key, double[] data, Jedis jedis){
			jedis.del(key);
			for(Double i : data )
			jedis.rpush(key, i.toString());
		}
		public void pushpoint(String key, ArrayList<Point> data, Jedis jedis){
			jedis.del(key);
			for(Point i : data )
			jedis.rpush(key, ((Integer)i.x).toString() +","+ ((Integer)i.y).toString());
		}
		public void pushStringList(String key, ArrayList<String> data, Jedis jedis){
			jedis.del(key);
			for(String i : data )
			jedis.rpush(key, i);
		}
		public void pushStringSet(String key, Set<String> data, Jedis jedis){
			jedis.del(key);
			for(String i : data )
			jedis.sadd(key, i);
		}
		
		
		public void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
			// For each jedis instance, disconnect it
			//for (Jedis jedis : jedisMap.values()) {
			//	jedis.disconnect();
			//}
		}
	} // end RedisRecordWriter
} // end RedisHashOutputFormat