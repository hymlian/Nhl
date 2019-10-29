package com.xxl.job.executor.jobhandler;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.sagittarius.bean.result.DoublePoint;
import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.service.CacheService;
import com.xxl.job.executor.util.DateRegUtil;
import com.xxl.job.executor.util.ListUtil;

import net.tycmc.bulb.common.util.DateUtil;
import tsinghua.thss.sdk.core.Client;
import tsinghua.thss.sdk.read.Reader;


@JobHandler(value="CacheJobHandler")
@Component
public class CacheJobHandler extends IJobHandler{

	@Autowired
	public CacheService cacheService;
	
	@Autowired
	public ListUtil listUtil;
	
	@Autowired
	@Qualifier("KmxClient")
	private Client Kmxclient;
	
	@Override
	public ReturnT<String> execute(String arg0) throws Exception {
		// TODO Auto-generated method stub
		XxlJobLogger.log(DateUtil.toString(new Date())+",缓存数据统计开始：");
		Map<String,Object>maxIdMap=cacheService.getcachelsexectableMaxId(arg0);
		if(MapUtils.isEmpty(maxIdMap)){
			XxlJobLogger.log("缓存数据统计结束：本次统计没有缓存数据");
			return SUCCESS;
		}
		String maxid=MapUtils.getString(maxIdMap,"maxid");
		String minid=MapUtils.getString(maxIdMap,"minid");
		//获取缓存表CLSET_Flag为0（未处理）的数据
		List<Map<String,Object>> cacheList = cacheService.getcachelsexectableList(arg0);
		//缓存数据日期分组
		Map<String, List<Map<String, Object>>> groupBy = listUtil.groupBy("CLSET_LastMsgTime", cacheList);
		
		Double MsgESS_iWork = 0.0 ;
		Double MsgESS_tWork = 0.0 ;
		Double MsgESS_iOilCons = 0.0; 
		Double MsgESS_tOilCons = 0.0; 
		Double MsgESS_iIdleWorkTime_PGN = 0.0;
		Double MsgESS_tIdleWorkTime_PGN = 0.0;
		Double MsgESS_iIdleWorkTime_UDS = 0.0;
		Double MsgESS_tIdleWorkTime_UDS = 0.0;
		
		Double MsgESM_iWork = 0.0 ;
		Double MsgESM_tWork = 0.0 ;
		Double MsgESM_iOilCons = 0.0; 
		Double MsgESM_tOilCons = 0.0; 
		Double MsgESM_iIdleWorkTime_PGN = 0.0;
		Double MsgESM_tIdleWorkTime_PGN = 0.0;
		Double MsgESM_iIdleWorkTime_UDS = 0.0;
		Double MsgESM_tIdleWorkTime_UDS = 0.0;
		//缓存月表集合
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		//更新缓存表参数集合
		List<Map<String, Object>> list1 = new ArrayList<Map<String, Object>>();
		//缓存自然月集合
		List<Map<String, Object>> list2 = new ArrayList<Map<String, Object>>();
		Map<String,Object>mapUpdateVcl=new HashMap<String,Object>();
		/**
		 * groupBy（例）
		 * 2017-12-31=[{CLSET_ID=3, CLSET_VclId=2000136, CLSET_LastMsgTime=2017-12-31, 
		 * CLSET_InsertTime=2018-10-17 14:59:24.0, CLSET_Flag=0, CLSET_InsertDate=2018-10-17}, 
		 * {CLSET_ID=10, CLSET_VclId=2000136, CLSET_LastMsgTime=2017-12-31, CLSET_InsertTime=2018-10-18 11:33:51.0, 
		 * CLSET_Flag=0, CLSET_InsertDate=2018-10-18}]
		 */
		for (Entry<String, List<Map<String, Object>>> entry : groupBy.entrySet()){
			String time = entry.getKey();
			String t = time.substring(0,7);
			String t1 = t.substring(0,4);
			String t2 = t.substring(5,7);
			String day =t1+t2;
			//获取当前时间
			Date date = new Date();
			SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");
			String newday = sdf.format(date);
			String ny = newday.substring(0, 7);
			
			List<Map<String, Object>> strvalue = entry.getValue();
			String BeginTime= time+" 00:00:00";
			String EndTime = time+" 23:59:59";
			
			
			Date parse = sdf.parse(time);
			Date firstDayDateOfMonth = CacheService.getFirstDayDateOfMonth(parse);
			Date lastDayOfMonth = CacheService.getLastDayOfMonth(parse);
			//传入时间所在月的第一天
			String format = sdf.format(firstDayDateOfMonth);
			//传入时间所在月的最后一天
			String format2 = sdf.format(lastDayOfMonth);
			
			String BeginTime1= format+" 00:00:00";
			String EndTime1 = format2+" 23:59:59";
			
			
			ArrayList<String> deviceIds = new ArrayList<>();
			for (Map<String, Object> entry1 : strvalue) {
				
				String CLSET_VclId = MapUtils.getString(entry1,"CLSET_VclId");
				
				String vclid="1003"+CLSET_VclId;
				deviceIds.add(vclid);
				mapUpdateVcl.put(CLSET_VclId,"1");
			}
			
			
			Reader reader = Kmxclient.getReader();
			ArrayList<String> sensorIds = new ArrayList<>();
			sensorIds.add("NHL_0002_00_247");// 累计工作小时
			sensorIds.add("NHL_0002_00_250");// 累计油耗
			sensorIds.add("NHL_0002_00_236");// 累计怠速耗油量
			sensorIds.add("NHL_0002_00_235");// 累计怠速时长
			long startTime = 0;
			long startTime1 = 0;
			try {
//			startTime = TimeUtil.string2Date(BeginTime);
				startTime = DateRegUtil.GetTimesTampByLTime(BeginTime);
				startTime1 = DateRegUtil.GetTimesTampByLTime(BeginTime1);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long endTime = 0;
			long endTime1 = 0;
			try {
//			endTime = TimeUtil.string2Date(EndTime);
				endTime = DateRegUtil.GetTimesTampByLTime(EndTime);
				endTime1 = DateRegUtil.GetTimesTampByLTime(EndTime1);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			Map<String, Map<String, List<DoublePoint>>> result = null;
			Map<String, Map<String, List<DoublePoint>>> result1 = null;
			try {
				result = reader.getDoubleRange(deviceIds, sensorIds, startTime, endTime, false);
				//是本月不重新统计自然月				
				if(!ny.equals(t)){
					result1 = reader.getDoubleRange(deviceIds, sensorIds, startTime1, endTime1, false);
				}
			} catch (NoHostAvailableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (QueryExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			/**
			 * 缓存月表
			 */
			if(result!=null){
				
				for (Map.Entry<String, Map<String, List<DoublePoint>>> reList : result.entrySet()) {
					String strkey = reList.getKey();
					String strid = strkey.substring(4);
					
					SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
					// 当前年月日
					String tday = s.format(date);
					
					Map<String,Object> map=new HashMap<String,Object>();
					map.put("CLSET_VclId",strid);
					map.put("CLSET_LastMsgTime", time);
					list1.add(map);
					
					String MsgEss_uniqueness=strid+"-"+time;
					
					System.out.println("strkey"+strkey);
					Map<String, List<DoublePoint>> revalue = reList.getValue();
					
					
					
					for (Map.Entry<String, List<DoublePoint>> reList1 : revalue.entrySet()) {
						String key = reList1.getKey();//工况
						System.out.println("key"+key);
						
						double first = 0;
						double last = 0;
						List<DoublePoint> value = reList1.getValue();
						
						switch (key) {
						case "NHL_0002_00_247":// 累计工作小时
//						for(DoublePoint va:value){
//							System.out.println("排序前：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							Collections.sort(value, new Comparator<DoublePoint>() {
								@Override
								public int compare(DoublePoint o1, DoublePoint o2) {
									// TODO Auto-generated method stub
									return (int) (o1.getValue()-o2.getValue());// 顺序
								}
							});
//						for(DoublePoint va:value){
//							System.out.println("排序后：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							first = value.get(0).getValue();
							last = value.get(value.size()-1).getValue();
							MsgESS_iWork=(last-first);
							MsgESS_tWork=last;
							
							break;
						case "NHL_0002_00_250":// 累计油耗
//						for(DoublePoint va:value){
//							System.out.println("排序前：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							Collections.sort(value, new Comparator<DoublePoint>() {
								@Override
								public int compare(DoublePoint o1, DoublePoint o2) {
									// TODO Auto-generated method stub
									return (int) (o1.getValue()-o2.getValue());// 顺序
								}
							});
//						for(DoublePoint va:value){
//							System.out.println("排序后：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							
							first = value.get(0).getValue();
							last = value.get(value.size()-1).getValue();
							MsgESS_iOilCons=(last-first);
							MsgESS_tOilCons=last;
							
							break;
						case "NHL_0002_00_236":// 累计怠速耗油量
//						for(DoublePoint va:value){
//							System.out.println("排序前：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							Collections.sort(value, new Comparator<DoublePoint>() {
								@Override
								public int compare(DoublePoint o1, DoublePoint o2) {
									// TODO Auto-generated method stub
									return (int) (o1.getValue()-o2.getValue());// 顺序
								}
							});
//						for(DoublePoint va:value){
//							System.out.println("排序后：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							
							first = value.get(0).getValue();
							last = value.get(value.size()-1).getValue();
							MsgESS_iIdleWorkTime_PGN=(last-first);
							MsgESS_tIdleWorkTime_PGN=(last);
							
							break;
						case "NHL_0002_00_235":// 累计怠速时长
//						for(DoublePoint va:value){
//							System.out.println("排序前：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							Collections.sort(value, new Comparator<DoublePoint>() {
								@Override
								public int compare(DoublePoint o1, DoublePoint o2) {
									// TODO Auto-generated method stub
									return (int) (o1.getValue()-o2.getValue());// 顺序
								}
							});
//						for(DoublePoint va:value){
//							System.out.println("排序后：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							
							first = value.get(0).getValue();
							last = value.get(value.size()-1).getValue();
							MsgESS_iIdleWorkTime_UDS=(last-first);
							MsgESS_tIdleWorkTime_UDS=(last);
							
							break;
						} 	
						
					}
					
					
					Map<String, Object> hm = new HashMap<String, Object>();
					
					hm.put("MsgESS_VclID", strid);
					hm.put("MsgESS_kmsdeviceId", strkey);
					hm.put("MsgESS_StatisticsTime", time);
					
					hm.put("day", day);
					hm.put("MsgEss_uniqueness", MsgEss_uniqueness);
					hm.put("MsgESS_iWork", MsgESS_iWork);
					hm.put("MsgESS_tWork", MsgESS_tWork);
					hm.put("MsgESS_iOilCons", MsgESS_iOilCons);
					hm.put("MsgESS_tOilCons", MsgESS_tOilCons);
					hm.put("MsgESS_iIdleWorkTime_PGN", MsgESS_iIdleWorkTime_PGN);
					hm.put("MsgESS_tIdleWorkTime_PGN", MsgESS_tIdleWorkTime_PGN);
					hm.put("MsgESS_iIdleWorkTime_UDS", MsgESS_iIdleWorkTime_UDS);
					hm.put("MsgESS_tIdleWorkTime_UDS", MsgESS_tIdleWorkTime_UDS);
					list.add(hm);
				}
			}
			/**
			 * 缓存自然月
			 */
			if(result1!=null){
				
				for (Map.Entry<String, Map<String, List<DoublePoint>>> reList : result1.entrySet()) {
					String strkey = reList.getKey();
					String strid = strkey.substring(4);
					
					SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
					// 当前年月日
					String tday = s.format(date);
					
					Map<String,Object> map=new HashMap<String,Object>();
					map.put("CLSET_VclId",strid);
					map.put("CLSET_LastMsgTime", time);
					list1.add(map);
					
					
					System.out.println("strkey"+strkey);
					Map<String, List<DoublePoint>> revalue = reList.getValue();
					
					
					
					for (Map.Entry<String, List<DoublePoint>> reList1 : revalue.entrySet()) {
						String key = reList1.getKey();//工况
						System.out.println("key"+key);
						
						double first = 0;
						double last = 0;
						List<DoublePoint> value = reList1.getValue();
						
						switch (key) {
						case "NHL_0002_00_247":// 累计工作小时
//						for(DoublePoint va:value){
//							System.out.println("排序前：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							Collections.sort(value, new Comparator<DoublePoint>() {
								@Override
								public int compare(DoublePoint o1, DoublePoint o2) {
									// TODO Auto-generated method stub
									return (int) (o1.getValue()-o2.getValue());// 顺序
								}
							});
//						for(DoublePoint va:value){
//							System.out.println("排序后：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							first = value.get(0).getValue();
							last = value.get(value.size()-1).getValue();
							MsgESM_iWork=(last-first);
							MsgESM_tWork=last;
							
							break;
						case "NHL_0002_00_250":// 累计油耗
//						for(DoublePoint va:value){
//							System.out.println("排序前：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							Collections.sort(value, new Comparator<DoublePoint>() {
								@Override
								public int compare(DoublePoint o1, DoublePoint o2) {
									// TODO Auto-generated method stub
									return (int) (o1.getValue()-o2.getValue());// 顺序
								}
							});
//						for(DoublePoint va:value){
//							System.out.println("排序后：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							
							first = value.get(0).getValue();
							last = value.get(value.size()-1).getValue();
							MsgESM_iOilCons=(last-first);
							MsgESM_tOilCons=last;
							
							break;
						case "NHL_0002_00_236":// 累计怠速耗油量
//						for(DoublePoint va:value){
//							System.out.println("排序前：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							Collections.sort(value, new Comparator<DoublePoint>() {
								@Override
								public int compare(DoublePoint o1, DoublePoint o2) {
									// TODO Auto-generated method stub
									return (int) (o1.getValue()-o2.getValue());// 顺序
								}
							});
//						for(DoublePoint va:value){
//							System.out.println("排序后：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							
							first = value.get(0).getValue();
							last = value.get(value.size()-1).getValue();
							MsgESM_iIdleWorkTime_PGN=(last-first);
							MsgESM_tIdleWorkTime_PGN=(last);
							
							break;
						case "NHL_0002_00_235":// 累计怠速时长
//						for(DoublePoint va:value){
//							System.out.println("排序前：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							Collections.sort(value, new Comparator<DoublePoint>() {
								@Override
								public int compare(DoublePoint o1, DoublePoint o2) {
									// TODO Auto-generated method stub
									return (int) (o1.getValue()-o2.getValue());// 顺序
								}
							});
//						for(DoublePoint va:value){
//							System.out.println("排序后：value "+va.getValue()+" "+"primaryTime "+va.getPrimaryTime());
//						}
							
							first = value.get(0).getValue();
							last = value.get(value.size()-1).getValue();
							MsgESM_iIdleWorkTime_UDS=(last-first);
							MsgESM_tIdleWorkTime_UDS=(last);
							
							break;
						} 	
						
					}
					
					
					Map<String, Object> hm = new HashMap<String, Object>();
					
					hm.put("MsgESM_VclID", strid);
					
					hm.put("day", day);
					hm.put("MsgESM_iWork", MsgESM_iWork);
					hm.put("MsgESM_tWork", MsgESM_tWork);
					hm.put("MsgESM_iOilCons", MsgESM_iOilCons);
					hm.put("MsgESM_tOilCons", MsgESM_tOilCons);
					hm.put("MsgESM_iIdleWorkTime_PGN", MsgESM_iIdleWorkTime_PGN);
					hm.put("MsgESM_tIdleWorkTime_PGN", MsgESM_tIdleWorkTime_PGN);
					hm.put("MsgESM_iIdleWorkTime_UDS", MsgESM_iIdleWorkTime_UDS);
					hm.put("MsgESM_tIdleWorkTime_UDS", MsgESM_tIdleWorkTime_UDS);
					list2.add(hm);
				}
			}
		}
		//统计月表sql集合
		List<String> sqlList = cacheService.execMaintCacheService(list);
		List<String> sqlList2 = cacheService.execMaintMonthjhService(list2);
		System.out.println(sqlList);
		
		
		//判断sqllist的size
		if(sqlList.size()>0){
			//开始执行批量插入
			boolean flag=cacheService.execBatch(sqlList);
			
			if(true==flag){
				//List<String> sqllist = cacheService.execMaintCachelsService(list1);
				//cacheService.execBatch(sqllist);
				if(sqlList2.size()>0){
					cacheService.execBatch(sqlList2);
				}
				
				XxlJobLogger.log("执行成功，本次成功统计的sql条数为："+sqlList.size());
			}else{
				XxlJobLogger.log("执行失败，本次应统计的sql条数为："+sqlList.size());
			}
		}else{
			XxlJobLogger.log("执行成功，本次成功统计的sql条数为："+0);
		}
		if(mapUpdateVcl.size()>0){
			boolean flag=cacheService.execUpdate(mapUpdateVcl,maxid,minid);
			if(flag==true){
				XxlJobLogger.log("本次需更新："+mapUpdateVcl.size()+"台车，更新成功！");
			}else{
				XxlJobLogger.log("本次需更新："+mapUpdateVcl.size()+"台车，更新标识失败！");
			}
		}
		return SUCCESS;
	}

}
