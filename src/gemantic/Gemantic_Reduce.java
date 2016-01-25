package gemantic;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Gemantic_Reduce extends Reducer<Text, ObjectWritable, Text, ObjectWritable> {
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 *
	 * 	�ּ��ۼ���(������ 2015.04.13)
	 * 
	 *	���ེ Ű/���
	 *		����Ű : (��Ʈ��) url + device
	 *		������� :  (������Ʈs in ������Ʈ) {{bounce : "������", close :"������", ...destination : "������"..}, {bounce : "������", close :"������", ...destination : "������"..}....}
	 * 
	 * 		��������Ű : (��Ʈ��) url + device + ����������
	 * 		�������¹�� : (������Ʈ) {bounce : "������", close :"������", ...destination : "������"..}
	 * 		������ : ��Ÿ��� 2��
	 * 
	 * 	���ེ�÷ο�
	 * 		1. �����ü���� ���� touchgroups��� ����Ʈ�� �ִ´�.....touchgroups.add((PageData) value.get())
	 * 		2. ������ ������.
	 * 		3. ������ ����, ��ġ������ ������ �ϴ� ����� �ϰ� �̸� ���ο� ��ü(result)�� �Ҵ��Ѵ�....touch_an.analysis(keys, page.country, page.date, page.touchdatas,page.userID, exData);
	 * 		4. ������ ����, ���������� üũ�ϸ� �� �������� ������ �����Ѵ�. �� �������� �������� result.copy()�̰�  result���� ���ÿ� �������� �������� ����Ѵ�.
	 * 		5. ������ ����, ������ ù��°�̸� result�� output�� �����ϰ�, �ι�° �������� ������� result�� ���� ������ result�� output�� combine�Ѵ�. 4���� ������ �������� �����鵵 �����ѹ������ combine�Ѵ�.
	 * 		6. ������ ���� ��, ��ü���� output�� 4���� ������ �������������� write�Ͽ��� ���ེ�� �����Ѵ�. write�Լ��� RedisTouchOutputFormat�� ���ǵǾ��ִ�.
	 * 
	 *
	 *	�ΰ�����
	 *		1. ���ེ�� url+����̽� Ű ������ŭ ����ȴ�. �� Ű ������ŭ ���ེ�� ��ġ ������ ���°�ó�� ����ȴٴ� ���̴�.
	 *		2. ������� combine �Ѵٴ� ���� values�ȿ� �ִ� value���� �� ������Һ��� 1:1�� �ϳ��� ����ģ�ٴ� ���̴�.
	 *		3. values�� �� ���� ��Ұ� ��ġ�ϴ°͵��� �ִ�. ������� Ű���� beagledog.com/hello,samsung �̰� ���⿡ values�� 5���ε�, ���� 2���� close�϶� ����϶�,
	 *		�������� close 2���� �ش��ϴ� value�� ��ġ��, ������ ������ ����������� close�� Ű������ write�ϴ� ���̴�. �ٽ��ѹ��������� value�� ������ ���ǰ������ ���� �����ϴ�.
	 *		 
	 */
	 

	@Override
	public void reduce(Text key, Iterable<ObjectWritable> values,	Context context) throws IOException, InterruptedException {

//		System.out.println("key = "+key.toString());
		System.out.println("Reduce loop start ================================================================================================================================");

	
		Touch_Analysis touch_an = new Touch_Analysis();
		String[] keys;			// url, device
		
		
		//System.out.println("````````````key : "+key);
		keys = key.toString().split(",");		//url�� device ���� ����
		keys[1] = key.toString().substring(keys[0].length()+1);
		
		//db�� �����Ͽ� �ܺ� �����͸� �����´�
		ExternalData exData = null;		
		//�м� ����� ���� ��ü
		ResultData result = null;
		ResultData output = null;
		ResultData output_bounce = null;
		ResultData output_close = null;
		ResultData output_complete = null;
		ResultData output_destination = null;
		ResultData output_timehost = null;
		ResultData output_events = null;
		ResultData output_timepage = null;
		
		ArrayList<PageData> touchgroups = new ArrayList<PageData>();
		
		for (ObjectWritable value : values) {//��ü ���� (��ġ������ + �ܺε�����)
			
			//�̰��� ��ġ�����Ͷ��
			if(value.getDeclaredClass() == PageData.class){
				//System.out.println("get PD " + (PageData)value.get());
				touchgroups.add((PageData) value.get());
			}
			//�̰��� �ܺ� �����Ͷ��
			else if(value.getDeclaredClass() == ExternalData.class){
				//System.out.println("get No PD " + value.get());
				exData = (ExternalData)value.get();
			}
			
		}


		if(exData == null){//�ܺ� �����͸� �� ã�Ҵٸ�
			System.out.println(keys[0] + "�� " + keys[1] + "�� �����Ͱ� �������� �ʾ� �м��� ������ �� �����ϴ� - exData");
			return;
		}


		
		for (PageData page : touchgroups) {//�ϳ��� ��ġ������ ����
			
			/*
			System.out.println("����������[�ٿ]  " + page.bounce);
			System.out.println("����������[Ŭ����]  " + page.close);
			System.out.println("����������[����]  " + page.country);
			System.out.println("����������[��¥]  " + page.date);
			System.out.println("����������[������]  " + page.destination);
			System.out.println("����������[������ī��Ʈ]  " + page.destination_totalcount);
			System.out.println("����������[����̽�]  " + page.device);
			System.out.println("����������[�̺�Ʈ]  " + page.events);
			System.out.println("����������[�̺�Ʈī��Ʈ]  " + page.events_totalcount);
			System.out.println("����������[Ÿ��ȣ��Ʈ]  " + page.timehost);
			System.out.println("����������[Ÿ��ȣ��Ʈī��Ʈ]  " + page.timehost_totalcount);
			System.out.println("����������[Ÿ��������]  " + page.timepage);
			System.out.println("����������[Ÿ��������ī��Ʈ]  " + page.timepage_totalcount);
			System.out.println("����������[�ּ�]  " + page.url);
			System.out.println("����������[�������̵�]  " + page.userID);
			System.out.println("����������[��ġ������]  " + page.touchdatas.length);
			*/
			
			
			//System.out.println("in for");
			if(page == null){
				System.out.println("page is empty");
				continue;
			}	
			else if(page.country == null || page.country.length() == 0){
				System.out.println("country is empty");
				continue;
			}	
			
			
			
			if(page.touchdatas == null || page.touchdatas.length == 0){
				System.out.println("touchdatas are empty");
				continue;
			}
			
			result = touch_an.analysis(keys, page.country, page.date, page.touchdatas,page.userID, exData); //�ϳ��� ��ġ���� �м�		


			if(page.close == 1)
				result.close = true;
			
			if(page.bounce == 1)
				result.bounce = true;
			
			if(page.complete == 1)
				result.complete = true;
		

			
			if(page.destination != -1 && page.destination != 0 ){
				result.destination = true;
				result.destination_time = page.destination;
				result.destination_totalcount = page.destination_totalcount;
				
				
			}
			if(page.timehost != -1 && page.timehost != 0){
				result.timehost = true;
				result.timehost_time = page.timehost;
				result.timehost_totalcount = page.timehost_totalcount;
			
			}
			if(page.timepage != -1 && page.timepage != 0){
				result.timepage = true;
				result.timepage_time = page.timepage;
				result.timepage_totalcount = page.timepage_totalcount;
			}
			if(page.events != -1 && page.events != 0){
				result.events = true;
				result.events_time = page.events;
				result.events_totalcount = page.events_totalcount;
			}
			
			System.out.println("result fin");
			

			//ù��°���
			if(result.get_area_time() == null)
				continue;
			
			//ù��°����...
			if(output == null){
				
				
				output = result.copy();

				output.type=0;
			}
			//�ι�°��������...�ջ��Ѵ�.
			else{

				output = output.combine(output,result);
			}
			
			/*
			 * 
			 * �����α�(2015.04.13 ������)
			 * �Ĺ����Ҷ� �Ķ���� ������ ��������.
			 * 
			 * combine�Լ����� ������� Ÿ���� "ù��° �Ķ����"�� ������.
			 * �׷��� �Ʒ� if,else������ else�� ����� result������ type���� ���������� �ʴ»��´�
			 * ���������� result�� ù��° �Ķ���Ϳ�, output�� �ι�° �Ķ���Ϳ� �־���.
			 * ������ ������� result_destination = =true �ΰ�찡 2���̻��϶�, result�� ��ٷ� else������ ���� result���� type���� ����.
			 * output_destination = output_destination.combine(result. output_destination);	�̰� ����Ǵµ�
			 * output_destination.type�� ���� ������� ���ͼ� ���������� write�� �Ѿ�鼭 destination��ü�� ���谡 �ȵǴ� ������ �߻��߾���.
			 * �Ķ������ ������ �ٲ������ν� ������ �ذ��Ͽ���.
			 * 
			 * 
			 */
			
			if(result.bounce == true){
				//System.out.println("-----------------------bounce");
				if(output_bounce == null){
					output_bounce = result.copy();
					output_bounce.type=1;				
				}
				else
					output_bounce = output_bounce.combine(output_bounce,result);
				
				System.out.println(output_bounce.type);
			}
			
			if(result.close == true){
				//System.out.println("--------------------------close");
				if(output_close == null){
					output_close = result.copy();
					output_close.type=2;	
				}
				else
					output_close = output_close.combine(output_close,result);		
			}
			
			if(result.complete == true){
				//System.out.println("----------------------------complete");
				if(output_complete == null){
					output_complete = result.copy();
					output_complete.type=3;
				}
				else
					output_complete = output_complete.combine(output_complete, result);		
					
			}
			if(result.destination == true){
				//System.out.println("----------------------------destination");
				if(output_destination == null){
					output_destination = result.copy();
					output_destination.type=4;
					
				}
				else{
					output_destination = output_destination.combine(output_destination, result);	
				}
				
			}
			if(result.timehost == true){
				//System.out.println("----------------------------timehost");
				if(output_timehost == null){
					output_timehost = result.copy();
					output_timehost.type=5;
				}
				else
					output_timehost = output_timehost.combine(output_timehost,result);		
					//System.out.println(output_timehost.type);
			}
			if(result.events == true){
				//System.out.println("----------------------------events");
				if(output_events == null){
					output_events = result.copy();
					output_events.type=6;
				}
				else
					output_events = output_events.combine(output_events,result);		
				//Sstem.out.println(output_events.type);
			}
			if(result.timepage == true){
				//System.out.println("----------------------------timepage");
				if(output_timepage == null){
					output_timepage = result.copy();
					output_timepage.type=7;
				}
				else
					output_timepage = output_timepage.combine(output_timepage,result);		
				
			}
			
			
			
		}
		

		//������!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//�����ٳ����� ����������+������ҵ鳢��(ex bounce ,close etc)�����ļ� write��ɾ��ļ� �ƿ�ǲ�������� ����
		
		if(output != null){
			
			context.write(key, new ObjectWritable(output));
		}
		if(output_bounce != null){
			
			context.write(key, new ObjectWritable(output_bounce));
		}
		if(output_close != null){
			
			context.write(key, new ObjectWritable(output_close));
		}
		if(output_complete != null){
			
			context.write(key, new ObjectWritable(output_complete));
		}
		if(output_destination != null){
		
			context.write(key, new ObjectWritable(output_destination));
		}
		if(output_timehost != null){
			
			context.write(key, new ObjectWritable(output_timehost));
		}
		if(output_events != null){
			
			context.write(key, new ObjectWritable(output_events));
		}
		if(output_timepage != null){
			
			context.write(key, new ObjectWritable(output_timepage));
		}
	}	

}