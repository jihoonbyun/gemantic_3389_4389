package path;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


public class Gemantic_Path {

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
		job.setJarByClass(Gemantic_Path.class);
		job.setJobName("gemantic_path");

		System.out.println("Gemantic_Path");

		System.out.println("inputformat-start");
		//레디스 접속 모듈
		job.setInputFormatClass(RedisDatePathInputFormat.class);
		RedisDatePathInputFormat.setRedisHosts(job, args[0]); //터치디비
		//인풋단계에서도 잠시 메타디비 호스트 정보가 필요함.
		RedisDatePathInputFormat.RedisMetaHost(args[1].toString()); //결과디비
		RedisDatePathInputFormat.setRedisDateKey(job, args[2]); //날짜
		System.out.println("inputformat-finish");

		//FileInputFormat.addInputPath(job, new Path(args[0]));
//		System.out.println("outputpath-start");
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		System.out.println("outputpath-finish");

		System.out.println("mapset-start");
		job.setMapperClass(Gemantic_Path_Map.class);
		System.out.println("mapset-end");
		job.setReducerClass(Gemantic_Path_Reduce.class);
		System.out.println("reduceset-end");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ObjectWritable.class);

		System.out.println("outputtype-start");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ObjectWritable.class);
		System.out.println("outputtype-finish");
		

		System.out.println("outputformat-start");
		job.setOutputFormatClass(RedisPathOutputFormat.class);
		RedisPathOutputFormat.setRedisHosts(job,args[1]); //결과디비
		RedisPathOutputFormat.setRedisHosts_DB(job, args[0]); //터치디비
		
		//RedisPathOutputFormat.setRedisHosts(job, args[0]);
	//	RedisHashOutputFormat.setRedisHashKey(job, "result");
		System.out.println("outputformat-finish");


		System.out.println("exit-start");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("exit-end");
		
	}
} 

