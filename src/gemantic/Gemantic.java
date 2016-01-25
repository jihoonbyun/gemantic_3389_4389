package gemantic;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


public class Gemantic {

	public static void main(String[] args) throws Exception {
	
		/*분석 순서
		 * 
		 * PATH
		 * 
		 * 1. 날짜 기준으로 패스 DB에서 데이터 전부 콜
		 * 2. 분석하여 메타 DB에 넣는다.
		 * 3. 데이터 결과를 메타 DB에 넣으면서 해당 터치 데이터들의 이탈, 종료 플래그를 세운다.
		 * 
		 * INPAGE
		 * 
		 * 1. 날짜 기준으로 터치데이터 DB에서 데이터 전부 콜. 어제 데이터도 불러온다.
		 * 2. 데이터에 path 정보가 있는지 보고, undifiend 라면 처리하지 않는다.
		 * 3. 각각의 데이터에서 페이지 정보, 디바이스 이름을 보고 외부 데이터를 가져온다.
		 * 4. page data 와 external data 를 전부 hadoop에 밀어넣는다.
		 * 5. 분석한다.
		 * 6. 분석된 결과를 종료, 이탈, 완독 여부를 보고 각각의 합침을 계산한다.
		 * 7. 결과를 DB에서 같은 정보를 콜해 불렀다가 합쳐 다시 넣어놓는다.
		 * 
		 */
		
		Job job = new Job();
		job.setJarByClass(Gemantic.class);
		job.setJobName("gemantic");


		System.out.println("inputformat-start#");
		//레디스 접속 모듈
		job.setInputFormatClass(RedisTouchInputFormat.class);
		//thumbtics.com
		RedisTouchInputFormat.setRedisHosts(job, args[0]);
		//20160101
		RedisTouchInputFormat.setRedisDateKey(job, args[2]);
		
		System.out.println("inputformat-finish");

		
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
//		System.out.println("outputpath-start");
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		System.out.println("outputpath-finish");

		System.out.println("mapset-start");
		job.setMapperClass(Gemantic_Map.class);
		System.out.println("mapset-end");
		job.setReducerClass(Gemantic_Reduce.class);
		System.out.println("reduceset-end");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ObjectWritable.class);

		System.out.println("outputtype-start");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ObjectWritable.class);
		System.out.println("outputtype-finish");
		

		System.out.println("outputformat-start");
		job.setOutputFormatClass(RedisTouchOutputFormat.class);
		RedisTouchOutputFormat.setRedisHosts(job, args[1]);
		RedisTouchOutputFormat.setRedisHosts_DB(job, args[0]);
	//	RedisHashOutputFormat.setRedisHashKey(job, "result");
		System.out.println("outputformat-finish");


		System.out.println("exit-start");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("exit-end");
		
	}
} 

