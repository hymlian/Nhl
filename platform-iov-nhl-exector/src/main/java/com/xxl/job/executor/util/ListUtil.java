 package com.xxl.job.executor.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import java.util.Arrays;
import java.util.Collections;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.springframework.stereotype.Component;

import net.tycmc.bulb.common.util.DateUtil;
import net.tycmc.bulb.common.util.MapGetter;
import net.tycmc.bulb.common.util.StringUtil;

/**
 * List工具类
 * @author jiyongtian
 **/

@Component
public class ListUtil {
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public ListUtil(){
		
	}
	/**
	 * 按照指定的map.key 将List组装为Map<key,List<Map<String,Object>>>
	 * @author haoyang
	 * 
	 **/
	public Map<String,List<Map<String,Object>>> groupBy(String key1,List<Map<String,Object>> srcList){
		Map<String,List<Map<String,Object>>> retMap = new HashMap<String, List<Map<String,Object>>>();
		String mapKey = "";
		List<Map<String,Object>> mapList = null;
		if(null!=srcList&&srcList.size()>0){
			for (Map<String, Object> map : srcList) {
				mapKey = MapGetter.getString(map, key1);
				mapList = retMap.get(mapKey);
				if(mapList==null){
					mapList = new ArrayList<Map<String,Object>>();
					mapList.add(map);
					retMap.put(mapKey.toString(), mapList);
				}else{
					mapList.add(map);
				}
			}
		}
		return retMap;
	}
	
	/**
	 * 该方法同groupBy，本方法为有序Map，因日工作时间分布需要而新增
	 * 20150914--LXF
	 * @param key1
	 * @param srcList
	 * @return
	 */
	public Map<String,List<Map<String,Object>>> groupByOrder(String key1,List<Map<String,Object>> srcList){
		Map<String,List<Map<String,Object>>> retMap = new ListOrderedMap();
		String mapKey = "";
		List<Map<String,Object>> mapList = null;
		if(null!=srcList&&srcList.size()>0){
			for (Map<String, Object> map : srcList) {
				mapKey = MapGetter.getString(map, key1);
				mapList = retMap.get(mapKey);
				if(mapList==null){
					mapList = new ArrayList<Map<String,Object>>();
					mapList.add(map);
					retMap.put(mapKey.toString(), mapList);
				}else{
					mapList.add(map);
				}
			}
		}
		return retMap;
	}
	
	/**
	 * 按照指定的map.key 将List组装为Map<key,List<Map<String,String>>>
	 * @author haoyang
	 * 
	 **/
	public Map<String,List<Map<String,String>>> groupBy(String key1,ArrayList<HashMap<String, String>> srcList){
		Map<String,List<Map<String,String>>> retMap = new HashMap<String, List<Map<String,String>>>();
		String mapKey = "";
		List<Map<String,String>> mapList = null;
		if(null!=srcList&&srcList.size()>0){
			for (Map<String, String> map : srcList) {
				mapKey = map.get(key1);
				mapList = retMap.get(mapKey);
				if(mapList==null){
					mapList = new ArrayList<Map<String,String>>();
					mapList.add(map);
					retMap.put(mapKey.toString(), mapList);
				}else{
					mapList.add(map);
				}
			}
		}
		return retMap;
	}
	/**
	 * 按照指定的map.key1_map.key2  将List组装为Map<map.key1_map.key2,List<Map<String,Object>>>
	 * @author jiyongtian
	 * 
	 **/
	public Map<String, List<Map<String,Object>>> groupByKeys(String key1,
			String key2, List<Map<String, Object>> srcList) {
		Map<String,List<Map<String,Object>>> retMap = new HashMap<String, List<Map<String,Object>>>();
		List<Map<String,Object>> mapList = null;
		if(null!=srcList&&srcList.size()>0){
			for (Map<String, Object> map : srcList) {
				StringBuffer mapKey = new StringBuffer();
				mapKey.append(MapGetter.getString(map, key1)+"_"+ MapGetter.getString(map, key2));
				mapList = retMap.get(mapKey.toString());
				if(null==mapList||mapList.size()<=0){
					mapList = new ArrayList<Map<String,Object>>();
					mapList.add(map);
					retMap.put(mapKey.toString(), mapList);
				}else{
					mapList.add(map);
				}
			}
		}
		return retMap;
	}
	/**
	 * 按照指定的map.key1_map.key2  将List组装为Map<map.key1_map.key2,List<Map<String,Object>>>
	 * @author jiyongtian
	 * 
	 **/
	public Map<String, List<Map<String,Object>>> groupByKeysOrder(String key1,
			String key2, List<Map<String, Object>> srcList) {
		Map<String,List<Map<String,Object>>> retMap = new ListOrderedMap();
		List<Map<String,Object>> mapList = null;
		if(null!=srcList&&srcList.size()>0){
			for (Map<String, Object> map : srcList) {
				StringBuffer mapKey = new StringBuffer();
				mapKey.append(MapGetter.getString(map, key1)+"_"+ MapGetter.getString(map, key2));
				mapList = retMap.get(mapKey.toString());
				if(null==mapList||mapList.size()<=0){
					mapList = new ArrayList<Map<String,Object>>();
					mapList.add(map);
					retMap.put(mapKey.toString(), mapList);
				}else{
					mapList.add(map);
				}
			}
		}
		return retMap;
	}

	/**
	 * 按照指定的map.key1_map.key2  将List组装为Map<map.key1_map.key2,List<Map<String,String>>>
	 * @author jiyongtian
	 * 
	 **/
	public Map<String, List<Map<String, String>>> groupBy(String key1,
			String key2, ArrayList<HashMap<String, String>> srcList) {
		Map<String,List<Map<String,String>>> retMap = new HashMap<String, List<Map<String,String>>>();
		List<Map<String,String>> mapList = null;
		if(null!=srcList&&srcList.size()>0){
			for (Map<String, String> map : srcList) {
				StringBuffer mapKey = new StringBuffer();
				mapKey.append(map.get(key1)+"_"+ map.get(key2));
				mapList = retMap.get(mapKey.toString());
				if(null==mapList||mapList.size()<=0){
					mapList = new ArrayList<Map<String,String>>();
					mapList.add(map);
					retMap.put(mapKey.toString(), mapList);
				}else{
					mapList.add(map);
				}
			}
		}
		return retMap;
	}
	/**
	 *简单的类型转换
	 *@author jiyongtian
	 **/
	public static List<List<String>> arrayToList(List<String[]> srcList){
		List<List<String>> reList = new ArrayList<List<String>>();
		for(String[] one:srcList){
			List<String> oneList = Arrays.asList(one);
			reList.add(oneList);
		}
		return reList;
	}
	/**
	 *第一个、最后一个有效值的差值
	 *@author jiyongtian
	 *@return 
	 **/
	public double getFirstAndLastValidValue(List<Map<String,Object>> srcList,String key ){
		double value0 = 0;
		double value1 = 0;
		for (Map<String,Object> curr : srcList) {
			if(StringUtil.isNullOrEmpty(MapGetter.getString(curr, key))){
				value0 = Double.parseDouble(MapGetter.getString(curr, key));
				break;
			}
		}
		for(int i=srcList.size()-1;i>=0;i--){
			if(StringUtil.isNullOrEmpty(MapGetter.getString(srcList.get(i), key))){
				value1 = Double.parseDouble(MapGetter.getString(srcList.get(i), key));
				break;
			}
		}
		return  value1 - value0;
	}
	/**
	 *获取List<?>中的最大值
	 *@param List<?> srcList: 目前仅支持List<String>和ArrayList<List<String>> 两种，否则返回0.0
	 *@author jiyongtian
	 **/
	public double getDataMaxVal(List<?> srcList,String type) {
		List<Double> tempDou = new ArrayList<Double>();
		if(null!=srcList){
			Iterator<?> it = srcList.iterator();
			if(!StringUtil.isValid(type)){
				type= "0";
			}
			while(it.hasNext()){
				Object next = it.next();
				if(next instanceof java.util.ArrayList<?> ){//ArrayList类型
					ArrayList<?> tempList = (ArrayList<?>)next;
					tempDou.add(new ListUtil().getDataMaxVal(tempList, type));
				}else if(next instanceof java.lang.String){//String 类型
					String tempStr = (String)next;
					switch (Integer.parseInt(type)){
					case 0:
						tempDou.add(Double.parseDouble(tempStr));
						break;
					case 1:
						tempDou.add(Double.parseDouble(tempStr.substring(0,tempStr.indexOf("%"))));
						break;
					case 2:
						if(tempStr.indexOf("%")>-1){
							tempDou.add(Double.parseDouble(tempStr.substring(0,tempStr.indexOf("%")))/100);
						}else{
							tempDou.add(Double.parseDouble(tempStr));
						}
						break;
				    default: 
				    	break;
					}
				}else if(next instanceof java.util.Map){
					Map<String,?> map =(Map<String,?>)next;
					if(null!=map.get(type)&&map.get(type).toString().length()>0){
						tempDou.add(Double.parseDouble(map.get(type).toString()));
					}else{
						tempDou.add(0.0);
					}
				}
			}
		}
		if(!tempDou.isEmpty()&&tempDou.size()>0){
			return Collections.max(tempDou);
		}else{
			return 0.0;
		}
	}
	/**
	 * 判断List<?>数据是否是含"%"形式
	 * @param List<?> srcList: 目前仅支持List<String>和ArrayList<List<String>> 两种，否则返回0
	 * @return int reInt:0   不是百分比形式；1   全部是百分比形式；2 部分是百分比形式
	 **/
	public int checkPercent(List<?> dataList) {
		boolean flag = false;
		int reInt = 0;
		int i=0;
		Iterator<?> it = dataList.iterator();
		while(it.hasNext()){
			Object next = it.next();
			if(next instanceof java.util.ArrayList<?> ){//ArrayList类型
				ArrayList<?> tempList = (ArrayList<?>)next;
				reInt = new ListUtil().checkPercent(tempList);
				if(reInt==2){
					break;
				}
			}else if(next instanceof java.lang.String ){//String 类型
				String temp = (String)next;
				if(i==0){//第一条数据记录时初始化
					if(temp.indexOf("%")>-1){
						flag = true;
					}
				}else{
					if(flag){
						if(temp.indexOf("%")<0){
							reInt = 2;
							break;
						}
						if(reInt==0){
							reInt = 1;
						}
					}else{
						if(temp.indexOf("%")>-1){
							reInt = 2;
							break;
						}
					}
				}
			}
			i++;
		}
		return reInt;
	}
	/**
	 * 用","拼接一个指定key对应的value值
	 **/
	public static String getAppendValuesByOneKey(
			List<Map<String, Object>> srcList,String key) {
		StringBuffer reBuf = new StringBuffer();
		if(null!=srcList&&srcList.size()>0){
			for(int i= 0;i<srcList.size();i++){
				Map<String,Object> oneMap = srcList.get(i);
				if(i>0){
					reBuf.append(","+MapGetter.getString(oneMap, key));
				}else{
					reBuf.append(MapGetter.getString(oneMap, key));
				}	
			}
		}
		return reBuf.toString();
	}
	/**
	 * List<Map<String,Object>>排序 仅支持Map.value 类型为int\date
	 * @param String keyField 排序时使用的Map.key
	 * @param boolean isTime 该key对应的value是否是“时间” 
	 * @param boolean desc 是否是倒叙
	 * @author jiyongtian 2014-03-17
	 * @更新历史 jiyongtian 20140723 增加判断源数据中获取的value不能为null或者“”的判断
	 **/
	public void sortList(List<Map<String,Object>> srcList, final String keyField, final boolean isTime,final boolean desc){
		Collections.sort(srcList, new Comparator<Map<String,Object>>() {
			@Override
			public int compare(Map<String, Object> arg0,Map<String, Object> arg1) {
				int i = 0;
				if (isTime){
					try
					{
						String starttemp = "";
						String endtemp = "";
						long l = 0;
						if(desc){
							starttemp = arg0.get(keyField).toString();
							endtemp = arg1.get(keyField).toString();
							if(StringUtil.isValid(starttemp)&&StringUtil.isValid(endtemp)){
								l = ListUtil.sdf.parse(endtemp).getTime() - ListUtil.sdf.parse(starttemp).getTime();
							}
						}else{
							starttemp = arg1.get(keyField).toString();
							endtemp = arg0.get(keyField).toString();
							if(StringUtil.isValid(starttemp)&&StringUtil.isValid(endtemp)){
								l= ListUtil.sdf.parse(endtemp).getTime() - ListUtil.sdf.parse(starttemp).getTime();
							}
						}
						if (l > 0L){
							i = 1;
						}else if (l == 0L){
							i = 0;
						}else{
							i = -1;
						}
					}catch (ParseException parseexception) { 
						
					}
				}else{
					if(StringUtil.isValid(arg0.get(keyField))&&StringUtil.isValid(arg1.get(keyField))){
						try
						{
							int turn0 = Integer.parseInt(arg0.get(keyField).toString());
							int turn1 = Integer.parseInt(arg1.get(keyField).toString());
							if(desc){
								i = turn1 - turn0;
							}else{
								i = turn0 - turn1;
							}
						}catch (NumberFormatException numberformatexception) { 
							
						}catch (NullPointerException nullpointerexception) { 
							
						}
					}else{
						//如果从源数据中获取 value值时为null或者“”，默认为两个map相等，程序无需处理
					}
				}
				return i;
			}
		});
	}
	/**
	 * 截取时间在指定时间段内的List
	 * @param List<Map<String,Object>> dataList 任意顺序
	 * @更新历史 jiyongtian 20140723 增加判断源数据中获取的value不能为null或者“”的判断
	 **/
	public List<Map<String, Object>> subBetweenTimes(
			List<Map<String, Object>> dataList,String timeKey, String[] timeDef) {
		List<Map<String,Object>> reList = null;
		if(null!=dataList && dataList.size()>0){
			reList = new ArrayList<Map<String,Object>>();
			try {
				if(StringUtil.isValid(timeDef[0])||StringUtil.isValid(timeDef[1])){
					long BgnTime = sdf.parse(timeDef[0]).getTime();
					long EndTime = sdf.parse(timeDef[1]).getTime();
					for(Map<String,Object> map:dataList){
						if(StringUtil.isValid(map.get(timeKey).toString())){
							long ibgn = sdf.parse(map.get(timeKey).toString()).getTime();
							if(BgnTime > ibgn){
								continue;
							}else{
								//如果位置点时间大于结束时间
								if(EndTime<ibgn){
									continue;
								}else{
									reList.add(map);
								}
							}
						}else{//如果获取时间value为null，默认直接将这个时间map放到返回值里
							reList.add(map);
						}
					}
				}else{//如果指定的时间范围有任意一个为null，则直接返回源数据。
					reList = dataList;
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		return reList;
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		List<String>list=new ArrayList<String>();
		list.add("1");
		list.add("2");
		list.add("3");
	}
}