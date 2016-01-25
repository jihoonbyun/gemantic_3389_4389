package gemantic;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


public class Gemantic {

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
		job.setJarByClass(Gemantic.class);
		job.setJobName("gemantic");


		System.out.println("inputformat-start#");
		//���� ���� ���
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

