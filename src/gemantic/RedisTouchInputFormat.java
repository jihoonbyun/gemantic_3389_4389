package gemantic;

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
public class RedisTouchInputFormat extends InputFormat<Text, ObjectWritable> {

	//Again, the CSV list of hosts and a hash key variables and methods for configuration
	public static final String REDIS_HOSTS_CONF = "mapred.redishashinputformat.hosts";
	public static final String REDIS_DATE_KEY_CONF = "mapred.redishashinputformat.date";

	//IP ���
	public static void setRedisHosts(Job job, String hosts) {
		job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
		System.out.println("	-	input path : "+ hosts);
	}
	
	//��¥ ���
	public static void setRedisDateKey(Job job, String Date) {
		job.getConfiguration().set(REDIS_DATE_KEY_CONF, Date);
		System.out.println("	-	input date : "+ Date);
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
	public RecordReader<Text, ObjectWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new RedisHashRecordReader();
	}

	//This custom RecordReader will pull in all key/value pairs from a Redis instance for a given hash
	public static class RedisHashRecordReader extends RecordReader<Text, ObjectWritable> {

		//A number of member variables to iterate and store key/value pairs from Redis
		private Iterator<Entry<String, Object>> keyValueMapIter = null;
		private Text key = new Text();
		ObjectWritable value = new ObjectWritable();
		private float processedKVs = 0, totalKVs = 0;
		private Entry<String, Object> currentEntry = null;

		//Initialize is called by the framework and given an InputSplit to process
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {

			//Get the host location from the InputSplit
			String host = split.getLocations()[0];
			String date = ((RedisHashInputSplit) split).getDate();

			//Create a new connection to Redis
			//@SuppressWarnings("resource")
			Jedis jedis = new Jedis(host, 3389);
			jedis.connect();
			jedis.getClient().setTimeoutInfinite();

			//Get all the key/value pairs from the Redis instance and store them in memory
			
			Integer count = 0;

			//System.out.println("----------------init");
			//date�� �ش��ϴ� ��� �������鿡 ���� ������ �����´�
			Map<String, Object> inputs =		makeinputs(jedis, date, count);

			totalKVs = inputs.size();
			keyValueMapIter = inputs.entrySet().iterator();
			System.out.println("Got " + totalKVs + " from " + date); jedis.disconnect();
		}
		
		//�� �������� ��ġ�����͵��� �����´�.
		public Touch_Data[] makeTouchdatas(Map <String,String> returndata, Jedis jedis){
			Touch_Data[] touchs;
			Touch_Data start = new Touch_Data();
			Touch_Data end = new Touch_Data();
			Touch_Data move;
			Set<String> moveshashs;
			List<String> movedatas;
			List<String> touchdatas;
			
			String orietation = returndata.get("orientation");
			
			//�� ����� ���� �������?
			if(!("0".equals(orietation))){
				touchs = new Touch_Data[0];//������ ���� - �� ���� ������
				return touchs;
			}
			
			
			jedis.select(1);
			
			String gesture = returndata.get("gesture");
			int gesturenum = 1;
		//	System.out.println("gesture : " + gesture);
			if("tab".equals(gesture)){
				gesturenum = 1;
			}
			else if("doubletab".equals(gesture)){
				gesturenum = 2;
			}
			else if("longtab".equals(gesture)){
				gesturenum = 3;
			}
			else if("drag".equals(gesture)){
				gesturenum = 4;
			}
			else if("pinch".equals(gesture)){
				gesturenum = 5;
			}
			else if("spread".equals(gesture)){
				gesturenum = 6;
			}
			else if("copy".equals(gesture)){
				gesturenum = 7;
			}

			touchdatas = jedis.hmget(returndata.get("start"), "pageX", "pageY", "clientX", "clientY", "timeStamp");
	
			start.page.x = Integer.parseInt(touchdatas.get(0));
			start.page.y = Integer.parseInt(touchdatas.get(1));
			start.client.x = Integer.parseInt(touchdatas.get(2));
			start.client.y = Integer.parseInt(touchdatas.get(3));
			start.time = Long.parseLong(touchdatas.get(4));
			start.gesture = gesturenum;
			
			start.type=1;
			
			touchdatas = jedis.hmget(returndata.get("end"), "pageX", "pageY", "clientX", "clientY", "timeStamp");
		
			try{
				end.page.x= Integer.parseInt(touchdatas.get(0));
			}
			catch(NumberFormatException e){
				end.page.x = (int)((Math.round(Double.parseDouble(touchdatas.get(0)))));
			}
			try{
				end.page.y= Integer.parseInt(touchdatas.get(1));
			}
			catch(NumberFormatException e){
				end.page.y = (int)((Math.round(Double.parseDouble(touchdatas.get(1)))));
			}
			try{
				end.client.x= Integer.parseInt(touchdatas.get(2));
			}
			catch(NumberFormatException e){
				end.client.x = (int)((Math.round(Double.parseDouble(touchdatas.get(2)))));
			}
			try{
				end.client.y= Integer.parseInt(touchdatas.get(3));
			}
			catch(NumberFormatException e){
				end.client.y = (int)((Math.round(Double.parseDouble(touchdatas.get(3)))));
			}
			

			
			try{
				end.time = Integer.parseInt(touchdatas.get(4));
			}
			catch(NumberFormatException e){
				end.time = (long)(Double.parseDouble(touchdatas.get(4)));
			}

			
			
			end.gesture = gesturenum;
			
			end.type=2;
			
			//�巡�׿����� ���갡 ����
			if("drag".equals(gesture)){
				int i=1;
				jedis.select(1);
				moveshashs = jedis.smembers(returndata.get("move"));
				touchs = new Touch_Data[2+moveshashs.size()];
				touchs[0] = start;
				touchs[1 + moveshashs.size()] = end;
				
				for(String st : moveshashs){
					move = new Touch_Data();
					movedatas = jedis.hmget(st, "pageX", "pageY", "clientX", "clientY", "timeStamp");
					
					
					
					
					try{
						move.time = Integer.parseInt(touchdatas.get(4));
					}
					catch(NumberFormatException e){
						move.time = (long)(Double.parseDouble(touchdatas.get(4)));
					}
				
					
				
					
					try{
						move.page.x= Integer.parseInt(movedatas.get(0));
					}
					catch(NumberFormatException e){
						move.page.x = (int)((Math.round(Double.parseDouble(touchdatas.get(0)))));
					}
					
					
					
					try{
						move.page.y= Integer.parseInt(movedatas.get(1));
					}
					catch(NumberFormatException e){
						move.page.y = (int)((Math.round(Double.parseDouble(touchdatas.get(1)))));
					}
					
					
					
					try{
						move.client.x= Integer.parseInt(movedatas.get(2));
					}
					catch(NumberFormatException e){
						move.client.x = (int)((Math.round(Double.parseDouble(touchdatas.get(2)))));
					}
					
					
					
					try{
						move.client.y= Integer.parseInt(movedatas.get(3));
					}
					catch(NumberFormatException e){
						move.client.y = (int)((Math.round(Double.parseDouble(touchdatas.get(3)))));
					}
					
					
					move.type=2;
					move.gesture = gesturenum;
					touchs[i++] = move;
				}
				Touch_Data tmp;
				
				for(i=1; i < moveshashs.size()+1 ; i++){
					for(int j=i;j< moveshashs.size()+1 ; j++){
						if(touchs[i].time > touchs[j].time){
							tmp = touchs[i];
							touchs[i]=touchs[j];
							touchs[j] = tmp;
						}
					}
				}
				
			}
			else{

				touchs = new Touch_Data[2];	
				touchs[0]=start;
				touchs[1]=end;
				
			}
			
			return touchs;
			
		}
		
		public Touch_Data[] makeTouchDataArray(ArrayList<Touch_Data[]> touchs){
			Touch_Data[] Toucharray;
			Touch_Data[] Touchs;
			Touch_Data[] Touchs_i;
			Touch_Data[] Touchs_j;
		
			Object [] TD = new Object[touchs.size()];
			
			for(int i=0;i<touchs.size();i++){
				TD[i] = touchs.get(i);
			}
			
			//Touch_Data [][] TD = new Touch_Data[10][touchs.size()];
			
			//����
			for(int i=0;i<TD.length;i++){
				for(int j=i;j<TD.length;j++){
					if(((Touch_Data[])(TD[i]))[0].time > ((Touch_Data[])(TD[j]))[0].time){
						Touchs_i = ((Touch_Data[])(TD[i]));
						Touchs_j = ((Touch_Data[])(TD[j]));
						TD[i] = Touchs_j;
						TD[j] = Touchs_i;
					}
				}
			}


			//�迭 ����, ����
			int cnt = 0;
			for(Object st : TD){
				cnt += ((Touch_Data[])st).length;
			}
			
			int i,j,num=0;
			Toucharray = new Touch_Data[cnt];
			for(i=0;i<TD.length;i++){
				Touchs = (Touch_Data[])(TD[i]);
				for(j=0;j<Touchs.length;j++){
					Toucharray[num++] = Touchs[j];
				}
			}
			
			return Toucharray;
			
		}
		
		public Map<String, Object> makeinputs(Jedis jedis, String date, Integer count){
			
			//�� ��¥�� ��� �����͸� ��, DB 0
			
			jedis.select(0);

			Set<String> allpages = jedis.keys(date+"*");
			
			
			Map<String, Object> pages = new HashMap<String, Object>();
			Map<String, ArrayList<Touch_Data[]>> touchdatamap = new HashMap<String, ArrayList<Touch_Data[]>>();
			String[] pagehashkeys;
			ArrayList<Touch_Data[]> newtouchdatalist;
			Touch_Data[] newtouchdata;
			PageData newpage;
			Map <String,String> returndata;
			
			//������ �������� ���Ͽ�
			for(String pagehash : allpages){
				pagehashkeys = pagehash.split(":");

			//	System.out.println("-----------------------pagehash = " + pagehash);
				jedis.select(0);
				//���� �ҷ�����
				returndata = jedis.hgetAll(pagehash);
				if("".equals(returndata.get("gesture"))){
					System.out.println("err for null data");
					continue;
				}
				
				//ù��°
				//20150327:10:407ab39-be35-43fd-89cb-ca0510309b3w4:��ġŰ
				//��¥:�ð�:����Ű:��ġŰ
				if(touchdatamap.get(pagehashkeys[2]) == null){//�����̸�(mukem) �� Ű��
					//�� ������ ����, ���� �Է�
					//System.out.println("--------------------new mukem key = " + pagehashkeys[2]);
					newpage = new PageData();
					newpage.country = returndata.get("country");
					newpage.date = Integer.parseInt(pagehashkeys[0]);
					newpage.device = returndata.get("device");
					newpage.url = returndata.get("url");
					newpage.userID = returndata.get("clientid");
					pages.put(pagehashkeys[2], newpage);
					
					//�� �����ĵ� ����
					newtouchdatalist = new ArrayList<>();
				
					newtouchdata = makeTouchdatas(returndata,jedis);
					
					//�����ĸ� ����Ʈ�� �ֱ�
					newtouchdatalist.add(newtouchdata);
					
					//����Ʈ�� �ʿ� �ֱ�
					touchdatamap.put(pagehashkeys[2], newtouchdatalist);
				}
				else{
					//System.out.println(pagehashkeys[2] + "----------------add!");
					newtouchdatalist = touchdatamap.get(pagehashkeys[2]);
					
					//�� �����ĵ� ����
					newtouchdata = makeTouchdatas(returndata,jedis);
					
					//�����ĸ� ����Ʈ�� �ֱ�
					newtouchdatalist.add(newtouchdata);
				}
			}
			
			Set<String> keyset = touchdatamap.keySet();
			ArrayList<Touch_Data[]> touchdataset;
			ArrayList<String> deleteListString = new ArrayList<>();
			
			//System.out.println("keyset size = " + keyset.size());

			//���� Ÿ�� üũ
			for(String st : keyset){ // st == mukem
				
				touchdataset = touchdatamap.get(st);
				
				for(Touch_Data[] td : touchdataset){
					if(td.length == 0){//���� �����Ͱ� �ִٴ� ��.
						
						deleteListString.add(st);
						break;
					}
				
				}
							

			}
			
			for(String del : deleteListString){
				System.out.println("delete key for false data : " + del);
				pages.remove(del);
				touchdatamap.remove(del);
			}
			
			
			//Ÿ�� üũ, ���� �� �迭ȭ �ϱ�
			/*
			 * ����� 8�� : �����7���� �����͸� gemantic_path�� ������ �����Ǵ� �����͵��̴�.
			 * ����� 8������ "����Ű"�ν�, ������ �����̴�
			 * ex) 36ac3r-sfsefes-sefsef-sfsfs <-- beagledog.kr/samsung �ش��ϴ� ����Ű��.
			 * �����̹Ƿ�, ���� url�� �ش��ϴ� ����Ű�� �翬�� �������ϼ��ִ�.
			 */
	
			//ex)
			jedis.select(8);
			String typecheck;
			String counts;
			for(String st : keyset){ // st == mukem
				touchdataset = touchdatamap.get(st);
				
				
				//����, �迭ȭ
				((PageData)(pages.get(st))).touchdatas=makeTouchDataArray(touchdataset);
				typecheck = jedis.hget(st, "bounce");

				//System.out.print("bounce key = " + typecheck + " ");
				if(typecheck != null && !typecheck.equals("-1") && !typecheck.equals("0")){
					((PageData)(pages.get(st))).bounce = 1;
					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~`mukem key " + st + "is bounce!");
				}
				typecheck = jedis.hget(st, "close");
				//System.out.println("close key = " + typecheck + " ");
				if(typecheck != null && !typecheck.equals("-1") && !typecheck.equals("0")){
					((PageData)(pages.get(st))).close = 1;
					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~`mukem key " + st + "is close!");
				}
				//System.out.println("close key = " + typecheck + " ");
				typecheck = jedis.hget(st, "complete");
				if(typecheck != null && !typecheck.equals("-1") && !typecheck.equals("0")){
					((PageData)(pages.get(st))).complete = 1;
					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~`mukem key " + st + "is complete!");
				}

				typecheck = jedis.hget(st, "destination_elapsed_time");
				counts = jedis.hget(st, "destination_totalcount");
				//System.out.println("close key = " + typecheck + " ");
				if(typecheck != null && !typecheck.equals("-1") && !typecheck.equals("0")){
					((PageData)(pages.get(st))).destination = Integer.parseInt(typecheck);
					((PageData)(pages.get(st))).destination_totalcount = Integer.parseInt(counts);
					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~`mukem key " + st + "is destination!");
				}

				typecheck = jedis.hget(st, "timehost_elapsed_time");
				counts = jedis.hget(st, "timehost_totalcount");
				//System.out.println("close key = " + typecheck + " ");
				if(typecheck != null && !typecheck.equals("-1") && !typecheck.equals("0")){
					((PageData)(pages.get(st))).timehost = Long.parseLong(typecheck);
					((PageData)(pages.get(st))).timehost_totalcount = Integer.parseInt(counts);
					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~`mukem key " + st + "is timehost!");
				}
				typecheck = jedis.hget(st, "timepage_elapsed_time");
				counts = jedis.hget(st, "timepage_totalcount");
				//System.out.println("close key = " + typecheck + " ");
				if(typecheck != null && !typecheck.equals("-1") && !typecheck.equals("0")){
					((PageData)(pages.get(st))).timepage = Long.parseLong(typecheck);
					((PageData)(pages.get(st))).timepage_totalcount = Integer.parseInt(counts);
					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~`mukem key " + st + "is timepage!");
				}

				typecheck = jedis.hget(st, "events_elapsed_time");
				counts = jedis.hget(st, "events_totalcount");
	
				//System.out.println("close key = " + typecheck + " ");
				if(typecheck != null && !typecheck.equals("-1") && !typecheck.equals("0")){
					((PageData)(pages.get(st))).events = Long.parseLong(typecheck);
					((PageData)(pages.get(st))).events_totalcount = Integer.parseInt(counts);
					System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~`mukem key " + st + "is events!");
				}
				
			}
			
			//External Data �߰�
			Map <String, ExternalData> external = new HashMap<String, ExternalData>();
			ExternalData exdata;
			
			
			String key_ex;
			String key_hash;
			
			List<String> returnexdata;
			String tags;

			jedis.select(6);
			
			for(String st : keyset){

				jedis.select(6);
				key_ex = ((PageData)(pages.get(st))).url + "," + ((PageData)(pages.get(st))).device;
				if(external.get(key_ex) == null){
					exdata =  new ExternalData();
					exdata.url = ((PageData)(pages.get(st))).url;
					exdata.device = ((PageData)(pages.get(st))).device;
					
					//DB���� �ڷ� �ҷ�����
					//Ŭ���̾�Ʈ, ������ XY
					key_hash = MakeUrl(exdata);
					

					System.out.println(key_hash);
					
					
					returnexdata = jedis.hmget(key_hash + ":summary", "windowsize_width","windowsize_height","page_width","page_height","average_time");
					//TODO ���� ������ ���� ����
					//exdata.input_area(Integer.parseInt(returnexdata.get(0)), Integer.parseInt(returnexdata.get(1)), Integer.parseInt(returnexdata.get(2)), Integer.parseInt(returnexdata.get(3)));
					
					//������ �м��� ���� ���ٸ�? �� �ܺ��±״� �м��� ���� ����
					if(returnexdata.get(0) == null){
						continue;
					}
					
					//int dw,dh,pw,ph;
				//	System.out.println(returnexdata);
				//	System.out.println(returnexdata.get(0));
					
//					dw = Integer.parseInt(returnexdata.get(0));
//
//					dh = Integer.parseInt(returnexdata.get(1));
//					pw = Integer.parseInt(returnexdata.get(2));
//					ph = Integer.parseInt(returnexdata.get(3));
					
					//System.out.println("exData : " + dw + " " + dh + " " + pw + " " + ph);
					//System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! keyhash = " + key_hash);
					exdata.input_area(Integer.parseInt(returnexdata.get(0)), Integer.parseInt(returnexdata.get(1)), Integer.parseInt(returnexdata.get(2)), Integer.parseInt(returnexdata.get(3)));
					
					//���ü���ð� �޾ƿ���
					try{
						exdata.average_time = Double.parseDouble(returnexdata.get(4));
								
					}catch(Exception e){//���� ��� -1
						exdata.average_time = -1.0;
					}
				//	System.out.println("~~~~~~~~~~~~~~~~~AVERAGETIME : " + exdata.average_time);

					jedis.select(13);
					tags = jedis.get(MakeOnlyUrl(exdata) + ":currentNode");
					System.out.println("!!!!!!!!!!!!!! " + MakeOnlyUrl(exdata)+ ":currentNode");
					//�±� ������
					exdata.input_Tag(tags);
					
					external.put(key_ex, exdata);
				}
			}
			
			keyset = external.keySet();

			//pages�� ����
			for(String st : keyset){
				pages.put(st, external.get(st));
			}
			
			
			return pages;
		}
		
		public String MakeUrl(ExternalData exData){
			String key_hash;
			
			String[] urls = exData.url.split("/");
			key_hash = urls[0] + ":/";
			
			//System.out.println("urlsize = " + urls.length);
			//System.out.println(key_hash);
			
			
			for(int i=1;i<urls.length;i++){
				key_hash += urls[i];
				key_hash += "/";
			//	System.out.println(key_hash);
			}
			
			key_hash += ":" + exData.device;
			
			return key_hash;
			
		}
		
		public String MakeOnlyUrl(ExternalData exData){
			String key_hash;
			
			String[] urls = exData.url.split("/");
			key_hash = urls[0] + ":/";
			
			//System.out.println("urlsize = " + urls.length);
			//System.out.println(key_hash);
			
			
			for(int i=1;i<urls.length;i++){
				key_hash += urls[i];
				key_hash += "/";
			//	System.out.println(key_hash);
			}
			
			//key_hash += ":" + exData.device;
			
			return key_hash;
			
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