package gemantic;

import java.awt.Point;

//터치 데이터들의 정보를 지니는 클래스
public class Touch_Data {
	int gesture;
	int type;//1-처음, 2-마지막, 3-무브
	long time;
	int time_mili;
	Point client;
	Point page;
	int push_tag;
//	int[] show_area;
//	ArrayList<Integer> show_tag;
	
	public Touch_Data() {
		client = new Point();
		page = new Point();
	}
	
	//기능 : 한 줄의 스트링으로 된 터치데이터를 여러 변수로 구분한다.
	//인자 : 터치데이터 문자열
	//리턴 : 없음
	public static Touch_Data splitdata(String touchdata){
	
		Touch_Data data = new Touch_Data();
		
		data.gesture = Integer.parseInt(touchdata.substring(0,1));	
		data.type = Integer.parseInt(touchdata.substring(1,2));		
		data.time = Long.parseLong(touchdata.substring(2,15));
		data.time_mili = Integer.parseInt(touchdata.substring(12,15));
		data.client.x = Integer.parseInt(touchdata.substring(15,19));
		data.client.y = Integer.parseInt(touchdata.substring(19,23));
		data.page.x = Integer.parseInt(touchdata.substring(23,27));
		data.page.y = Integer.parseInt(touchdata.substring(27,31));
		data.push_tag = Integer.parseInt(touchdata.substring(31,35));

		return data;
	}

}
