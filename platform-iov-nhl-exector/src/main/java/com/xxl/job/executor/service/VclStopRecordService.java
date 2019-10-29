package com.xxl.job.executor.service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;
import com.sagittarius.bean.common.Row;
import com.sagittarius.bean.common.SortType;
import com.sagittarius.bean.query.Shift;
import com.sagittarius.bean.result.DoublePoint;
import com.sagittarius.bean.result.StringPoint;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.NoMetricException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.sagittarius.read.iterator.ITableIterator;
import com.sagittarius.util.TimeUtil;
import com.xxl.job.executor.dao.VclStopRecordDao;

import javafx.util.Pair;
import net.tycmc.bulb.common.util.DateUtil;
import tsinghua.thss.sdk.bean.common.Point;
import tsinghua.thss.sdk.core.Client;
import tsinghua.thss.sdk.exception.NotImplementedException;

@Component
public class VclStopRecordService {
	
	@Autowired
	private VclStopRecordDao vclStopRecordDao;
	
	@Value("${EngineRev}")
	private String EngineRev;

	@Value("${GPSSpeed}")
	private String GPSSpeed;

	@Value("${KeyOnOff}")
	private String KeyOnOff;
	
	@Autowired
	@Qualifier("KmxClient")
	private Client Kmxclient;
	/**
	 * 日工作时间分布表
	 * @param condition(时区条件）
	 * @data  日期
	 * 
	 *  逻辑处理：
	 *  	1、根据钥匙开关判断开关机状态，
	 *  	2、关机：整车上电
	 *  	3、开机：发动机转速 > 400 
	 *  					GPS车速 > 0 行驶状态
	 *  					GPS车速  < 0 未行驶状态
	 *  		        发动机车速  <400 整车上电
	 *  0:整车断电   1:整车上电  2:行驶  3启动未行驶
	 * @return
	 * @throws NotImplementedException 
	 * @throws NoMetricException 
	 * @throws TimeoutException 
	 * @throws QueryExecutionException 
	 * @throws NoHostAvailableException 
	 * @throws ParseException 
	 */
	public List<Map<String,Object>> VclStopRecord() throws NoHostAvailableException, QueryExecutionException, TimeoutException, NoMetricException, NotImplementedException, ParseException{ 
		
		String beginTime =DateUtil.addDay(DateUtil.toString(DateUtil.now(), "yyyy-MM-dd"),-1)+" 00:00:00";
		String endTime =  DateUtil.addDay(DateUtil.toString(DateUtil.now(), "yyyy-MM-dd"),-1)+" 23:59:59";

//		String beginTime = DateUtil.toString(DateUtil.now(), "yyyy-MM-dd");
//		String endTime = DateUtil.toString(DateUtil.now(), "yyyy-MM-dd");
		
		//1、 查询出需要统计的车辆信息
		List<Map<String, Object>> vehicleInfoList = vclStopRecordDao.getVehicleInfo();
		List<String> vclList = new ArrayList<String>();

		List<String>dutyNumber=new ArrayList<String>();
		
		if(null!=vehicleInfoList&&vehicleInfoList.size()>0) {
			
			Map<String,Map<String,String>> result = new HashMap<String, Map<String,String>>();
			
			List<String> sqlList = new ArrayList<String>();
			for(Map<String,Object> map:vehicleInfoList) {
				
				Map<String,String> stateMap = new LinkedHashMap<String, String>();
				
				String vehicleID ="1003"+ map.get("PFVehicleId")+"";
				
//				String vehicleID = "1003105100";
				vclList.clear();
				vclList.add(vehicleID);
				
				dutyNumber.clear();
				dutyNumber.add(KeyOnOff);
				//2. 从KMX 查询出车钥匙开关、GPS 车速数据
				
				List<Pair<Integer, SortType>> sortType = new ArrayList<Pair<Integer,SortType>>();
				sortType.add(new Pair<Integer, SortType>(-1, SortType.ASC));
				
//				Map<String, Map<String, List<Point>>> mapResult=Kmxclient.getReader().getRange(vclList, dutyNumber,beginTime, endTime);
				
				ITableIterator mapResult = Kmxclient.getReader().getRange(vclList, dutyNumber,beginTime, endTime,sortType,null);
				
				if(null!=mapResult) {
					
					while(mapResult.hasNext()) {
						Row row = mapResult.next();
						
						
						String value = row.getValue(KeyOnOff)+"";
						
						System.out.println("结果:"+value+" 第一时间:"+TimeUtil.date2String(row.getTimestamp(), "yyyy-MM-dd HH:mm:ss"));
						//开机 
						if("0".equals(value)) {

							dutyNumber.clear();
							dutyNumber.add(EngineRev);
							//转速信息
							DoublePoint revResult = Kmxclient.getReader().getFuzzyDoublePoint(vehicleID, EngineRev, row.getTimestamp(), Shift.BEFORE);
							if(null!=revResult) {
								double value2 = revResult.getValue();
								if(value2<400) {
									//0:整车断电   1:整车上电  2:行驶  3启动未行驶
									
									stateMap.put(row.getTimestamp()+"", "1");
								}else {
									StringPoint gpsPoint = Kmxclient.getReader().getFuzzyStringPoint(vehicleID, GPSSpeed, row.getTimestamp(), Shift.BEFORE);
									
									String gpsvalue = gpsPoint.getValue();
									if(!"".equals(gpsvalue)) {
										JSONObject obj = JSONObject.parseObject(gpsvalue);
										//车速信息
										String speed = obj.get("Speed")+"";
										if(!"".equals(speed)) {
											if(Double.valueOf(speed)>0) {
												//2
												stateMap.put(row.getTimestamp()+"", "2");
											}else {
												//3
												stateMap.put(row.getTimestamp()+"", "3");
											}
										}else {
											//3
											stateMap.put(row.getTimestamp()+"", "3");
										}
									}
								}
							}else {
								//1
								stateMap.put(row.getTimestamp()+"", "1");
							}
						}else {
							//关机
							stateMap.put(row.getTimestamp()+"", "0");
						}
					}
				}
				
				result.put(vehicleID, stateMap);
				
				
				String stat = "";
				String stime = "";
				String etime = "";
				boolean flag = false;
				
				//前一条状态
				String beforstat = "";
				//前一条时间
				String beforTime = "";
				for(String key : stateMap.keySet()) {
					if("".equals(beforstat)) {
						beforstat = stateMap.get(key);
						beforTime= TimeUtil.string2Date(TimeUtil.date2String(Long.valueOf(key),"yyyy-MM-dd")+" 00:00:00")+"";
						flag = true;
					}else {
						if(!beforstat.equals(stateMap.get(key))) {
							//如果状态不一样，将变化前的记录保存
							
							//保存记录的状态 
							stat = beforstat;
							//保存记录的开始时间
							stime = beforTime;
							//保存记录的结束时间
							etime = key;
							
							String keepHour = getSecond(TimeUtil.date2String(Long.valueOf(stime),"yyyy-MM-dd HH:mm:ss"),TimeUtil.date2String(Long.valueOf(etime),"yyyy-MM-dd HH:mm:ss"))+"";
							sqlList.add("insert into vclstoprecord_"+DateUtil.toString(DateUtil.toDate(beginTime,"yyyy-MM-dd"), "yyyyMM")+"(VSR_VCLID,VSR_EngState,VSR_BeginTime,VSR_EndTime,VSR_KeepHour)values('"+vehicleID.substring(4)+"','"+stat+"','"+TimeUtil.date2String(Long.valueOf(stime),"yyyy-MM-dd HH:mm:ss") +"','"+TimeUtil.date2String(Long.valueOf(etime),"yyyy-MM-dd HH:mm:ss")+"','"+keepHour+"')");
							
							beforstat = stateMap.get(key);
							beforTime= key;
							flag = false;  
						}else {
							etime = key;
							flag = true;  
						}
					}
				}
				
				
//				for(String key : stateMap.keySet()) {
//					if("".equals(stat)) {
//						stat = stateMap.get(key);
//						stime = key;
//						etime = key;
//						flag = true;
//					}else {
//						if(!stat.equals(stateMap.get(key))) {
//							stat = stateMap.get(key);
//							etime = key;
//							System.out.println(vehicleID+"<>"+stime+"<>"+etime+"<>"+stat);
//							String keepHour = getSecond(TimeUtil.date2String(Long.valueOf(stime),"yyyy-MM-dd HH:mm:ss"),TimeUtil.date2String(Long.valueOf(etime),"yyyy-MM-dd HH:mm:ss"))+"";
//							sqlList.add("insert into vclstoprecord_"+DateUtil.toString(DateUtil.toDate(beginTime,"yyyy-MM-dd"), "yyyyMM")+"(VSR_VCLID,VSR_EngState,VSR_BeginTime,VSR_EndTime,VSR_KeepHour)values('"+vehicleID+"','"+stat+"','"+TimeUtil.date2String(Long.valueOf(stime),"yyyy-MM-dd HH:mm:ss") +"','"+TimeUtil.date2String(Long.valueOf(etime),"yyyy-MM-dd HH:mm:ss")+"','"+keepHour+"')");
//							stime = key; 
//							flag = false;  
//						}else {
//							etime = key;
//							flag = true;
//						}
//					}
//				}

				if(flag) {
					String keepHour = getSecond(TimeUtil.date2String(Long.valueOf(beforTime),"yyyy-MM-dd HH:mm:ss"),TimeUtil.date2String(Long.valueOf(etime),"yyyy-MM-dd HH:mm:ss"))+"";
//					sqlList.add("insert into vclstoprecord_"+DateUtil.toString(DateUtil.toDate(beforTime,"yyyy-MM-dd"), "yyyyMM")+"(VSR_VCLID,VSR_EngState,VSR_BeginTime,VSR_EndTime,VSR_KeepHour)values('"+vehicleID+"','"+beforstat+"','"+TimeUtil.date2String(Long.valueOf(beforTime),"yyyy-MM-dd") +" 23:59:59','"+TimeUtil.date2String(Long.valueOf(etime),"yyyy-MM-dd HH:mm:ss")+"','"+keepHour+"')");
					sqlList.add("insert into vclstoprecord_"+DateUtil.toString(DateUtil.toDate(beforTime,"yyyy-MM-dd"), "yyyyMM")+"(VSR_VCLID,VSR_EngState,VSR_BeginTime,VSR_EndTime,VSR_KeepHour)values('"+vehicleID.substring(4)+"','"+beforstat+"','"+TimeUtil.date2String(Long.valueOf(beforTime),"yyyy-MM-dd HH:mm:ss") +"','"+TimeUtil.date2String(Long.valueOf(etime),"yyyy-MM-dd")+" 23:59:59','"+keepHour+"')");
				}
				
				int i  = vclStopRecordDao.batchInsert(sqlList);
				sqlList.clear();
				System.out.println("车辆："+vehicleID.substring(4)+" 执行结果为："+i);
			}
		}
		
		
		return null;
	}
	
	
	
	/**
	 *方法说明：计算两个时间差用秒数表示
	 *<br/>参数：两个时间
	 *<br/>返回值：十分秒的时间
	 */
	public static long getSecond(String s_date,String e_date){
		long miaoshu = 0;
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		if(s_date.length()==10){
			s_date = s_date+" 00:00:00";
		}
		if(e_date.length()==10){
			e_date = e_date+" 23:59:59";
		}
		try { 
			long s_date_long = formatter.parse(s_date).getTime();
			long e_date_long = formatter.parse(e_date).getTime();
			
			miaoshu = (e_date_long-s_date_long)/1000;
			
//			//如果有余数
//			if(miaoshu%2==1){
//				miaoshu = miaoshu+1;
//			}

		} catch (ParseException e) {
			System.out.println("类:DateControl 方法:getMiaoShu 执行:计算两个时间差用秒数表示 发生:ParseException异常");
		}
		return miaoshu;
	}
	
}
