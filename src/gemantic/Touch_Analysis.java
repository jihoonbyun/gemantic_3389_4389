package gemantic;

import java.awt.Point;
import java.util.ArrayList;

public class Touch_Analysis {
	
	ResultData result;		//	결과물을 담은 객체
	int[] show_area;
	ArrayList<Integer> show_tag;
	
	//상수들
	static int area_loc_time_gap = 50;	//	위치 정보 시간 간격 설정
	static int start = 1;
	static int end = 2;
	static int middle = 3;

	//기능 : 터치데이터 묶음을 받아 분석한다.
	//인자 : 터치데이터 묶음
	//리턴 : 없음
	public ResultData analysis(String[] keys, String country, int date, Touch_Data[] touchdatas, String userid, ExternalData exData){
		
		System.out.println("-------Touchdata Analysis start");
		System.out.println("URL : " + keys[0] + " height : " + exData.page_height + " size : " + exData.area_count);
		
		//System.out.println("devname = " + keys[1]);
		
		Touch_Data now_touch;
		result = new ResultData();
		//터치데이터 묶음 제일 앞에있는 url, 기기명, 국가, 날짜 입력
		result.make_Context(keys[0], keys[1], country, date, exData.area_count, exData.tag_count, exData.device_width, exData.device_height,userid);		

//		if(!keys[0].equals("mindjs.org/category/webtoon1/")){
//			return result;
//		
//		}
		
	//	System.out.println("``````````````````````this key = "+keys[1]);
//		if("korea".equals(country)){
//			result.bounce=true;
//		//	System.out.println("key = "+keys[0]+" bounce = "+result.bounce);
//		}
//		if("abc.com".equals(keys[0])){
//			result.close=true;
//		//	System.out.println("key = "+keys[0]+" close = "+result.close);
//		}
//		
//		System.out.println("터치 y");
//		for(int i = 0 ; i < touchdatas.length  ; i++  ){
//			System.out.println(touchdatas[i].);
//		}
		
		//System.out.println("in anal");
		int max = 0;
		int save_show = 0;
		int save_size = 50;
		
		
		int t=0;
		int k =0;	

		for(int i = 0 ; i < touchdatas.length  ; i++  ){
			if(touchdatas[i].gesture == 4){
				k++;
			}
		}

		
		
		for(int i = 0 ; i < touchdatas.length  ; i++  ){
			now_touch = touchdatas[i];
			//System.out.println(TouchDatas[i]);
			
		
			
			//새 데이터를 생성
			//now_touch = new Touch_Data();
			//splitdata(TouchDatas[i], now_touch);//터치데이터 하나를 분리
	//		System.out.println("y = " + now_touch.page.y);
			if(max < now_touch.page.y)
				max = now_touch.page.y;
				
			//현재 화면에 보이고 있는 구간, 구문들이 무엇인지 체크
			show_area = get_area(now_touch.page, now_touch.client, exData.area_height, exData.page_height, exData.device_height);
			
			if(show_area[0] == -1){
				show_area[0]=save_show;
				show_area[1]=save_size;	
			}
			else{
//				System.out.println("page - " + now_touch.page.x + " , " + now_touch.page.y + " client - " + now_touch.client.x + " , " + now_touch.client.y +
//						" top : " + (now_touch.page.y-now_touch.client.y) + " time = " + now_touch.time);
//				System.out.println("show height - " + show_area[0] + " ~ " + (show_area[0]+show_area[1]));
				save_show = show_area[0];
				save_size = show_area[1];				
			}
			
			//System.out.println("show " + i + " = " + show_area[0] + "size : " + show_area[1]);
			
			//이동 거리 계산
			area_move_count(now_touch.page, now_touch.client, exData.area_height, exData.page_height, exData.device_height, now_touch.type);
			show_tag = get_tag(now_touch.page, now_touch.client, exData.tag_offset, exData.tag_height, exData.page_height, exData.device_height);
			
			//제스쳐를 보고 카운트
			if(now_touch.type == start){//제스쳐 카운트는 start에서만 체크
			//	System.out.println("st");
				result.count_motion(now_touch.gesture);
				
				if(now_touch.gesture == 1){//탭일 때 실수처리, 히트맵 생성
					if(i != touchdatas.length - 2){//이게 마지막이라면, 즉 이 탭으로 다른 페이지로 갔을때만 링크 
						result.count_Fault_tab(now_touch.page.x, now_touch.page.y, exData.page_width, exData.page_height);
					}
//					
					result.count_Heat_Map(now_touch.page.x, now_touch.page.y, exData.page_width, exData.page_height);
					
//					int index = check_link(exData.link_list, now_touch.push_tag);
//					if(index == -1){//링크가 아닐때
//						result.count_Fault_tab(now_touch.page.x, now_touch.page.y, exData.page_width, exData.page_height);
//					}
////					else{//링크일때
////						result.count_link(index);
////					}
				}
				else if(now_touch.gesture == 2){//더블탭일 때 히트맵 생
					result.count_Heat_Map(now_touch.page.x, now_touch.page.y, exData.page_width, exData.page_height);
				}
				else if(now_touch.gesture == 3){//롱탭일 때 히트맵 생
					result.count_Heat_Map(now_touch.page.x, now_touch.page.y, exData.page_width, exData.page_height);
				}
				else if(now_touch.gesture == 4){//드래그일 때 드래그 시작 위치 저장
					result.count_Drag(now_touch.client.x, exData.device_width);
				}
				else if(now_touch.gesture == 6){//스프레드일때 선택된 태그에 셋
					result.count_Area_spread(now_touch.push_tag);
				}
				else if(now_touch.gesture == 7){//카피일때 선택된 태그에 셋
					result.count_Tag_copy(now_touch.push_tag);
				}
				
			}
			
			//상단메뉴를 탭한경우 피니시라인라인 0px에 다수 찍히는 현상이 발생하여, 터치데이터 타입이 드래그이면서 마지막 터치일때로 판단 으로 수정함
			//에리어라인도 0px로 되버리는 버그 있음
			//(2015.03.25- 변지훈)	
			//제스처가 드래그일떄
			//System.out.println("sssssssssss!!" + bef_area_start + "/" + drag_area_time);
			//stem.out.println("----터----치----대----마----왕---" + now_touch);
			
	
			
	//드래그일때만 판단한다.	
	//이유는, 상단탭메뉴를 탭했을때 위치 변화가 없어도 상단으로 올라간것으로 나오기때문	
	if(now_touch.gesture == 4){		
		
		t++;

		if(t == 1){//첫번째, 초기화
			//System.out.println("first");
				
			area_time_count(show_area[0],show_area[1], now_touch.time, start);
			area_loc_count(show_area[0], now_touch.time, start);
			tag_time_count(show_tag, now_touch.time, start, now_touch.page.y - now_touch.client.y);
			result.count_Area_start(bef_area_start, bef_area_time);//첫 터치의 정보 입력
		}
		
		if (t == k){//마지막, 마무리
			//System.out.println("Last");

			
			//드래그일때만 areaflow데이터를 담는다. 이유는, 상단탭메뉴를 탭했을때 위치 변화가 없어도 상단으로 올라간것으로 나오기때문			

			//터치시작점을 기준점으로 잡는다...
			area_time_count(show_area[0], show_area[1], now_touch.time, end);
			area_loc_count(show_area[0], now_touch.time, end);
			tag_time_count(show_tag, now_touch.time, end, now_touch.page.y - now_touch.client.y);
			
			//수정 :: bef_area_start => show_area[0]+show_area[1]
			//show_area[0] = 보이는 시작점 구간 위치, show_area[1] = 구간갯수. 피니시라인은 손끝이 끝난지점이여야 하므로 두개를 더해준다.
			result.count_Area_finish(show_area[0]+show_area[1], now_touch.time);//마지막 터치의 정보 입력
			
			//데이터 종합 정리
			result.merge_time();
			result.merge_Complete();
			result.merge_flow(exData.device_height, exData.area_height);
			result.merge_heat_map();
			result.merge_reading_pattern(exData.average_time);
			
			//초기화
			t =0;
			k= 0;
		}
		
		if(t > 1 && t < k){//일반적인 경우
	
				//System.out.println("middle");
				tag_time_count(show_tag, now_touch.time, middle, now_touch.page.y - now_touch.client.y);
				area_time_count(show_area[0], show_area[1], now_touch.time, middle);
				area_loc_count(show_area[0], now_touch.time, middle);
				
				
				//System.out.println("drag detected!" + bef_area_start_drag);
		}

	}//gesture ==4
			
		}
		System.out.println("Out! max = " + max);
		//분석이 끝나면 모은 분석 결과를 리턴
		return result;
	}

	
	/*	재석이코드....탭까지 인식하는 바람에 플로우랑 피니시라인이 이상해지는 것때매 주석처리 by 변지훈
	if(i == 0){//첫번째, 초기화
		//System.out.println("first");
			
		area_time_count(show_area[0],show_area[1], now_touch.time, start);
		area_loc_count(show_area[0], now_touch.time, start);
		tag_time_count(show_tag, now_touch.time, start, now_touch.page.y - now_touch.client.y);
		result.count_Area_start(bef_area_start, bef_area_time);//첫 터치의 정보 입력
	}
	
	if (i == touchdatas.length - 1){//마지막, 마무리
		//System.out.println("Last");

		
		//드래그일때만 areaflow데이터를 담는다. 이유는, 상단탭메뉴를 탭했을때 위치 변화가 없어도 상단으로 올라간것으로 나오기때문			

		area_time_count(show_area[0], show_area[1], now_touch.time, end);
		area_loc_count(show_area[0], now_touch.time, end);
		tag_time_count(show_tag, now_touch.time, end, now_touch.page.y - now_touch.client.y);
			
		System.out.println("the last touches!!!!!!!!!!!/ " + bef_area_start_drag + "/" + bef_area_time_drag);
		//맨마지막 데이터가아닌, 마지막 바로전의 데이터를 기준으로 계산한다. 이유는 finish의 경우 
		result.count_Area_finish(bef_area_start_drag, bef_area_time);//마지막 터치의 정보 입력
		
		//데이터 종합 정리
		result.merge_time();
		result.merge_Complete();
		result.merge_flow(exData.device_height, exData.area_height);
		result.merge_heat_map();
		result.merge_reading_pattern(exData.average_time);
	}
	
	if(i != 0 && i != touchdatas.length - 1){//일반적인 경우
		

	
		
		//드래그일때만 areaflow데이터를 담는다. 이유는, 상단탭메뉴를 탭했을때 위치 변화가 없어도 상단으로 올라간것으로 나오기때문			
		if(now_touch.gesture == 4){
			//System.out.println("middle");
			tag_time_count(show_tag, now_touch.time, middle, now_touch.page.y - now_touch.client.y);
			area_time_count(show_area[0], show_area[1], now_touch.time, middle);
			area_loc_count(show_area[0], now_touch.time, middle);
			
			bef_area_start_drag = bef_area_start;
			bef_area_time_drag = bef_area_time;
			//System.out.println("drag detected!" + bef_area_start_drag);
			
		}
	}
	*/
	
	
	//과거의 정보
	int bef_area_state;

	//기능 : 이동한 거리를 카운트한다.
	//인자 : 페이지 좌표, 클라이언트 좌표, 페이지 높이, 클라이언트 높이, 타입
	//리턴 : 없음
	public void area_move_count(Point page, Point client, int area_height, int page_height, int device_height, int type){
		int top;	//클라이언트 꼭대기가 페이지상에서 차지하는 좌표
		int move;
		//top = page.y - client.y;			
		//move = Math.abs(top - bef_area_state);
		//System.out.println("^^^^^^^^^^^^ pgy: " + page.y + "  cly : "+ client.y +  " top : " + top + " move : " + move);
		if(type == middle){	//일반적인 경우(중간)		
			if(0 < page.y && page.y <= page_height && 0 <= client.y && client.y <= device_height){
				//pagey = 
				top = page.y - client.y;			
				move = Math.abs(top - bef_area_state);
				result.count_Area_move(move);
				bef_area_state = top;
			}
			else
				System.out.println("y 값이 페이지 높이 범위를 벗어났습니다. 데이터에 이상이 있습니다 - area_move_count");
			//System.out.println("pagey::" + page.y + "/ clienty::" + client.y + "page_height:" + page_height + "device_height:" + device_height);					
		}
		else if (type == start){	//초기화
			if(0 < page.y && page.y <= page_height && 0 <= client.y && client.y <= device_height){
				top = page.y - client.y;			
				bef_area_state = top;
			}
			else{
				System.out.println("y 값이 페이지 높이 범위를 벗어났습니다. 데이터에 이상이 있습니다 - area_move_count");
				//System.out.println("pagey::" + page.y + "/ clienty::" + client.y + "page_height:" + page_height + "device_height:" + device_height);					bef_area_state = 0;
			}
		} else if (type == end) { // 마지막
			top = page.y - client.y;
			move = Math.abs(top - bef_area_state);
			result.count_Area_move(move);
			bef_area_state = top;

		}
		

	}
	
	
	
	//과거의 정보
	int bef_area_start;
	int bef_area_count;
	long bef_area_time;
	
	int first_area_start;
	int first_area_count;
	
	int end_area_start;
	int end_area_count;
	

	//기능 : 보고 있는 구간이 바뀌었는지 확인하고 바뀌었다면 구간 체류 시간을 올린다.
	//인자 : 현재 첫번째 구간의 인덱스, 현재 구간의 수, 시간, 타입(처음인가, 중간인가, 마지막인가)
	//리턴 : 없음
	public void area_time_count(int area_start, int area_count, long time, int type){
		int elapsed;//시간 차이
		
		if(type == middle){	//일반적인 경우(중간)			
		
			if(area_start != bef_area_start || area_count != bef_area_count){//보이는 구간에 변동이 있을 때
				elapsed = (int)(time - bef_area_time);	//시간차 계산
				result.count_Area_time(bef_area_start, bef_area_count, elapsed, bef_area_time);//그 만큼의 시간이 지난 것이므로 올린다

				//백업
				bef_area_start = area_start;
				bef_area_count = area_count;
				bef_area_time = time;
			}			
		}
		else if (type == start){	//초기화
			bef_area_start = area_start;
			bef_area_count = area_count;
			bef_area_time = time;
			
			first_area_start = area_start;
			first_area_count = area_count;
		}
		else if (type == end){	//마지막
			
			if( area_start < first_area_start ){//올라갔을때, 즉 back 일때
				result.count_Area_reverse(first_area_start, first_area_count);				
			}
	
			
			elapsed = (int)(time - bef_area_time);	//시간차 계산
			result.count_Area_time(bef_area_start, bef_area_count, elapsed, bef_area_time);//그 만큼의 시간이 지난 것이므로 올린다
			
			end_area_start = area_start;
			end_area_count = area_count;
	
		}
	}
	

	//과거의 정보
	long bef_loc_time;
	//기능 : 일정 시간이 지났는지 확인하고 지났다면 현재위치를 저장해 둔다.
	//인자 : 현재 첫번째 구간의 인덱스, 시간, 타입(처음인가, 마지막인가, 중간인가)
	//리턴 : 없음
	public void area_loc_count(int area_start, long time, int type){
		if(type == middle){
			//System.out.println("md time = " + time + " bef_time = " + bef_loc_time + " dif =  " + (time - bef_loc_time));
			if( time > 0 &&  time - bef_loc_time > area_loc_time_gap){
				for(; bef_loc_time < time ; bef_loc_time += area_loc_time_gap ){
					result.count_Area_loc(area_start);					
				}
			}		
		}
		else if(type == start){
		//	System.out.println("st");
			result.count_Area_loc(area_start);
			bef_loc_time = time;
		}
		else if(type == end){
		//	System.out.println("end");
			result.count_Area_loc(area_start);
		}
	}
	
	//기능 : 현재 좌표로 보고 있는 구간이 어딘지 판별한다.
	//인자 : 페이지상의 좌표, 클라이언트상의 좌표, 구간의 높이, 페이지의 높이, 디바이스의 높이
	//리턴 : 첫번째 구간의 인덱스, 구간의 수
	public int[] get_area(Point page, Point client, int area_height, int page_height, int device_height){
		int[] show_area = new int[2];
		int top;	//클라이언트 꼭대기가 페이지상에서 차지하는 좌표
		int start = 0;
		int finish;
		
		show_area[0]=-1;
		show_area[1]=-1;
		
		if(0 < page.y && page.y <= page_height && 0 <= client.y && client.y <= device_height){
			//pagey = 
			top = page.y - client.y;
			start = top/area_height;
			finish = (top + device_height) / area_height;
			if((top + device_height) % area_height == 0)
				finish--;
			
			show_area[0]=start;
			show_area[1]=finish-start + 1;
			
		}
		else
			System.out.println("y 값이 페이지 높이 범위를 벗어났습니다. 데이터에 이상이 있습니다 - get_area");
			//System.out.println("pagey::" + page.y + "/ clienty::" + client.y + "page_height:" + page_height + "device_height:" + device_height);		
		
		//System.out.println("start : " + start + " page : " + page.y + " client : " + client.y + " pageh : " + page_height + " devh : " + device_height);
		
		return show_area;
	}
	
	//과거의 정보
	ArrayList<Integer> bef_tag_show;
	long bef_tag_time;
	int bef_top;

	//기능 : 보고 있는 구문이 바뀌었는지 확인하고 바뀌었다면 구문 체류 시간을 올린다.
	//인자 : 보고 있는 구문의 배열, 시간, 타입(처음인가, 중간인가, 마지막인가)
	//리턴 : 없음
	public void tag_time_count(ArrayList<Integer> show_tag, long time, int type, int top){
	
		if(type == middle){//중간
			if(show_tag != null){
				if(!show_tag.equals(bef_tag_show)){//달라졌으면
					
					//역류
					if(top < bef_top){
						result.count_Tag_reverse(bef_tag_show, (int)(time - bef_tag_time));
					}
					
					result.count_Tag_time(bef_tag_show, (int)(time - bef_tag_time));
					bef_tag_show = show_tag;
					bef_tag_time=time;		
					bef_top = top;
				}
			}
			else
				System.out.println("show_tag가 null 입니다 - tag_time_count");
		}
		else if (type == start){//초기화
			bef_tag_show = show_tag;
			bef_tag_time=time;	
			bef_top = top;
		}
		else if (type == end){//마지막
			result.count_Tag_time(bef_tag_show, (int)(time - bef_tag_time));			
		}
		
		
	}

	//기능 : 현재 좌표로 보고 있는 구문이 어딘지 판별한다.
	//인자 : 페이지상의 좌표, 클라이언트상의 좌표, 구문들의 좌표, 구문들의 높이, 페이지의 높이, 디바이스의 높이
	//리턴 : 보고 있는 구문들
	public ArrayList<Integer> get_tag(Point page, Point client, int[] tag_offset, int[] tag_height, int page_height, int device_height){
		int top;
		int bottom;
		ArrayList<Integer> show_tag = new ArrayList<Integer>();
		
		if(0 < page.y && page.y <= page_height && 0 <= client.y && client.y <= page_height){
			top = page.y - client.y;
			bottom = top + device_height;
			
			for(int i=0 ; i < tag_offset.length;i++){
				if(top <= tag_offset[i] && tag_height[i] <= bottom){
					show_tag.add(i);
				}
			}			
			
		}
		else
			System.out.println("y 값이 페이지 높이 범위를 벗어났습니다. 데이터에 이상이 있습니다 - get_tag");
		
		
		return show_tag;
	}

//	//기능 : 현재 눌러진 태그가 링크인지 확인한다.
//	//인자 : 링크 인덱스 목록, 눌러진 태그의 인덱스
//	//리턴 : 링크일 경우 링크의 번호(링크목록의 인덱스), 없을 경우 -1
//	public int check_link (int[] link_list, int push_tag){
//		for(int i = 0; i < link_list.length ; i++){
//			if(link_list[i] == push_tag){//이 태그가 링크라면
//				return i;
//			}
//		}
//		return -1;
//	}
	

}
