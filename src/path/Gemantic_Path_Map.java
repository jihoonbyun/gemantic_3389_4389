package path;

import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Gemantic_Path_Map extends Mapper<Text, ObjectWritable, Text, ObjectWritable> {

	@Override
	public void map(Text key, ObjectWritable value, Context context) 
			throws IOException, InterruptedException {

		System.out.println("map");
		
		//key == site_url
		
//		//하나의 터치데이터 묶음
//		String str = value.toString();
//
//		//키값 추출 (url, device)
//		int chk=0;
//		int i;
//		for(i=0;i<str.length();i++){
//			if(str.charAt(i) == ','){
//				chk++;
//				if(chk == 2)
//					break;
//			}
//		}
//		
//		System.out.println("		-	map : " + str);
//		
//		PageData page = new PageData();
		
//		System.out.println(key.toString() + " direct");
		
		PathData ptd = null;
		if(value.getDeclaredClass() == PathData.class){
//			System.out.println("pagedata");
			ptd = (PathData) value.get();
			System.out.println("key = "+key.toString());
			System.out.println("url = "+ptd.site_url);

		}
//		else
//			System.out.println("not pagedata");
		
//		PageData pd = (PageData) value.get();
//		if(pd == null){
//			System.out.println("null?!");
//		}
//
//		System.out.println(pd.country + "  " + pd.date);
		
	//	PageData pp = new PageData();
		//System.out.println("map");
		//reduce로 넘기기
		context.write(new Text(ptd.site_url), new ObjectWritable(ptd));
		
		//output.collect(value, new Text ("v" + value.toString()));

	}
}