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
	 * 	주석작성자(변지훈 2015.04.13)
	 * 
	 *	리듀스 키/밸류
	 *		받은키 : (스트링) url + device
	 *		받은밸류 :  (오브젝트s in 오브젝트) {{bounce : "데이터", close :"데이터", ...destination : "데이터"..}, {bounce : "데이터", close :"데이터", ...destination : "데이터"..}....}
	 * 
	 * 		내보내는키 : (스트링) url + device + 컨버전스명
	 * 		내보내는밸류 : (오브젝트) {bounce : "데이터", close :"데이터", ...destination : "데이터"..}
	 * 		저장디비 : 메타디비 2번
	 * 
	 * 	리듀스플로우
	 * 		1. 밸류객체들을 전부 touchgroups라는 리스트에 넣는다.....touchgroups.add((PageData) value.get())
	 * 		2. 루프를 돌린다.
	 * 		3. 루프를 돌며, 터치데이터 묶음만 일단 계산을 하고 이를 새로운 객체(result)에 할당한다....touch_an.analysis(keys, page.country, page.date, page.touchdatas,page.userID, exData);
	 * 		4. 루프를 돌며, 컨버전스를 체크하며 각 컨버전스 변수를 생성한다. 각 컨버전스 변수들은 result.copy()이고  result에도 동시에 컨버전스 정보들을 등록한다.
	 * 		5. 루프를 돌며, 루프가 첫번째이면 result를 output에 저장하고, 두번째 루프부터 현재루프 result와 이전 루프의 result인 output을 combine한다. 4에서 생성한 컨버전스 변수들도 동일한방식으로 combine한다.
	 * 		6. 루프가 끝난 후, 전체값인 output과 4에서 생성한 컨버전스값들을 write하여서 리듀스를 종료한다. write함수는 RedisTouchOutputFormat에 정의되어있다.
	 * 
	 *
	 *	부가설명
	 *		1. 리듀스는 url+디바이스 키 갯수만큼 실행된다. 즉 키 갯수만큼 리듀스가 마치 루프를 도는것처럼 실행된다는 것이다.
	 *		2. 밸류들을 combine 한다는 것은 values안에 있는 value들을 각 구성요소별로 1:1로 하나로 다합친다는 것이다.
	 *		3. values들 중 서로 요소가 일치하는것들이 있다. 예를들어 키값이 beagledog.com/hello,samsung 이고 여기에 values가 5개인데, 이중 2개가 close일때 등등일때,
	 *		루프에서 close 2개에 해당하는 value를 합치고, 루프가 끝난후 최종결과물을 close를 키값으로 write하는 것이다. 다시한번말하지만 value의 갯수는 세션갯수라고 봐도 무방하다.
	 *		 
	 */
	 

	@Override
	public void reduce(Text key, Iterable<ObjectWritable> values,	Context context) throws IOException, InterruptedException {

//		System.out.println("key = "+key.toString());
		System.out.println("Reduce loop start ================================================================================================================================");

	
		Touch_Analysis touch_an = new Touch_Analysis();
		String[] keys;			// url, device
		
		
		//System.out.println("````````````key : "+key);
		keys = key.toString().split(",");		//url과 device 정보 지님
		keys[1] = key.toString().substring(keys[0].length()+1);
		
		//db와 연동하여 외부 데이터를 가져온다
		ExternalData exData = null;		
		//분석 결과를 담은 객체
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
		
		for (ObjectWritable value : values) {//전체 묶음 (터치데이터 + 외부데이터)
			
			//이것이 터치데이터라면
			if(value.getDeclaredClass() == PageData.class){
				//System.out.println("get PD " + (PageData)value.get());
				touchgroups.add((PageData) value.get());
			}
			//이것이 외부 데이터라면
			else if(value.getDeclaredClass() == ExternalData.class){
				//System.out.println("get No PD " + value.get());
				exData = (ExternalData)value.get();
			}
			
		}


		if(exData == null){//외부 데이터를 못 찾았다면
			System.out.println(keys[0] + "의 " + keys[1] + "의 데이터가 존재하지 않아 분석을 시작할 수 없습니다 - exData");
			return;
		}


		
		for (PageData page : touchgroups) {//하나의 터치데이터 묶음
			
			/*
			System.out.println("페이지묶음[바운스]  " + page.bounce);
			System.out.println("페이지묶음[클로즈]  " + page.close);
			System.out.println("페이지묶음[국가]  " + page.country);
			System.out.println("페이지묶음[날짜]  " + page.date);
			System.out.println("페이지묶음[도착지]  " + page.destination);
			System.out.println("페이지묶음[도착지카운트]  " + page.destination_totalcount);
			System.out.println("페이지묶음[디바이스]  " + page.device);
			System.out.println("페이지묶음[이벤트]  " + page.events);
			System.out.println("페이지묶음[이벤트카운트]  " + page.events_totalcount);
			System.out.println("페이지묶음[타임호스트]  " + page.timehost);
			System.out.println("페이지묶음[타임호스트카운트]  " + page.timehost_totalcount);
			System.out.println("페이지묶음[타임페이지]  " + page.timepage);
			System.out.println("페이지묶음[타임페이지카운트]  " + page.timepage_totalcount);
			System.out.println("페이지묶음[주소]  " + page.url);
			System.out.println("페이지묶음[유저아이디]  " + page.userID);
			System.out.println("페이지묶음[터치데이터]  " + page.touchdatas.length);
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
			
			result = touch_an.analysis(keys, page.country, page.date, page.touchdatas,page.userID, exData); //하나의 터치묶음 분석		


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
			

			//첫번째라면
			if(result.get_area_time() == null)
				continue;
			
			//첫번째루프...
			if(output == null){
				
				
				output = result.copy();

				output.type=0;
			}
			//두번째루프부터...합산한다.
			else{

				output = output.combine(output,result);
			}
			
			/*
			 * 
			 * 에러로그(2015.04.13 수정됨)
			 * 컴바인할때 파라미터 순서에 유의하자.
			 * 
			 * combine함수에서 결과물의 타입은 "첫번째 파라미터"를 따른다.
			 * 그런데 아래 if,else문에서 else에 들어갈경우 result값에는 type값이 설정되있지 않는상태다
			 * 수정전에는 result가 첫번째 파라미터에, output이 두번째 파라미터에 있었다.
			 * 때문에 예를들어 result_destination = =true 인경우가 2번이상일때, result가 곧바로 else문으로 들어가서 result에는 type값이 없다.
			 * output_destination = output_destination.combine(result. output_destination);	이게 실행되는데
			 * output_destination.type이 없는 결과물로 나와서 최종적으로 write로 넘어가면서 destination자체가 집계가 안되는 현상일 발생했었다.
			 * 파라미터의 순서르 바꿔줌으로써 문제를 해결하였다.
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
		

		//루프끝!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		//포문다끝나고 같은페이지+같은요소들끼리(ex bounce ,close etc)다합쳐서 write명령어쳐서 아웃풋포맷으로 보냄
		
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