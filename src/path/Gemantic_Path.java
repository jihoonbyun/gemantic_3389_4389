package path;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


public class Gemantic_Path {

	public static void main(String[] args) throws Exception {
	
		/*�м� ����
		 * 
		 * PATH
		 * 
		 * 1. ��¥ �������� �н� DB���� ������ ���� ��
		 * 2. �м��Ͽ� ��Ÿ DB�� �ִ´�.
		 * 3. ������ ����� ��Ÿ DB�� �����鼭 �ش� ��ġ �����͵��� ��Ż, ���� �÷��׸� �����.
		 * 
		 * INPAGE
		 * 
		 * 1. ��¥ �������� ��ġ������ DB���� ������ ���� ��. ���� �����͵� �ҷ��´�.
		 * 2. �����Ϳ� path ������ �ִ��� ����, undifiend ��� ó������ �ʴ´�.
		 * 3. ������ �����Ϳ��� ������ ����, ����̽� �̸��� ���� �ܺ� �����͸� �����´�.
		 * 4. page data �� external data �� ���� hadoop�� �о�ִ´�.
		 * 5. �м��Ѵ�.
		 * 6. �м��� ����� ����, ��Ż, �ϵ� ���θ� ���� ������ ��ħ�� ����Ѵ�.
		 * 7. ����� DB���� ���� ������ ���� �ҷ��ٰ� ���� �ٽ� �־���´�.
		 * 
		 */
		
		Job job = new Job();
		job.setJarByClass(Gemantic_Path.class);
		job.setJobName("gemantic_path");

		System.out.println("Gemantic_Path");

		System.out.println("inputformat-start");
		//���� ���� ���
		job.setInputFormatClass(RedisDatePathInputFormat.class);
		RedisDatePathInputFormat.setRedisHosts(job, args[0]); //��ġ���
		//��ǲ�ܰ迡���� ��� ��Ÿ��� ȣ��Ʈ ������ �ʿ���.
		RedisDatePathInputFormat.RedisMetaHost(args[1].toString()); //������
		RedisDatePathInputFormat.setRedisDateKey(job, args[2]); //��¥
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
		RedisPathOutputFormat.setRedisHosts(job,args[1]); //������
		RedisPathOutputFormat.setRedisHosts_DB(job, args[0]); //��ġ���
		
		//RedisPathOutputFormat.setRedisHosts(job, args[0]);
	//	RedisHashOutputFormat.setRedisHashKey(job, "result");
		System.out.println("outputformat-finish");


		System.out.println("exit-start");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.out.println("exit-end");
		
	}
} 

