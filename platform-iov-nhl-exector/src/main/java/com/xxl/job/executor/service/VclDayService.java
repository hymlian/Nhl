package com.xxl.job.executor.service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.sagittarius.exceptions.NoHostAvailableException;
import com.sagittarius.exceptions.NoMetricException;
import com.sagittarius.exceptions.QueryExecutionException;
import com.sagittarius.exceptions.TimeoutException;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.dao.VclDayDao;
import com.xxl.job.executor.util.DateRegUtil;
import com.xxl.job.executor.util.ListUtil;

import tsinghua.thss.sdk.bean.common.Point;
import tsinghua.thss.sdk.core.Client;
import tsinghua.thss.sdk.exception.NotImplementedException;

@Component
public class VclDayService {
	@Autowired
	private VclDayDao vclDayDao;
	
	@Value("${EngineWorkTime}")
	private String EngineWorkTime;
	
	@Value("${EngineIdeaOil}")
	private String EngineIdeaOil;
	
	@Value("${EngineIdeaWorkTime}")
	private String EngineIdeaWorkTime;
	
	@Value("${EngineOil}")
	private String EngineOil;
	
	@Value("${VclPreFiex}")
	private String VclPreFiex;
	
	@Autowired
	@Qualifier("KmxClient")
	private Client Kmxclient;
	/**
	 * 获取对应时区的设备
	 * @param condition(时区条件）
	 * @data  日期
	 * @return
	 */
	/**
	 * @param condition
	 * @return
	 * @throws ParseException 
	 */
	public List<Map<String,Object>> getExecVcl(String condition,String date){
		//获取对应时区的设备
		List<Map<String,Object>>list=vclDayDao.getExecVcl(condition);
		/*Map<String,Object>mapVcla=new HashMap<String,Object>();
		mapVcla.put("PfVehicleid","100001");
		list.add(mapVcla);*/
		//表中数据初始化
		Map<String,List<Map<String,Object>>> mapInfo=new ListUtil().groupBy("PfVehicleid", list);
		//获取这个时区内昨天设备的信息
		List<Map<String,Object>>listOil=vclDayDao.getLastDayVclOilInfo(condition, date);
		Map<String,List<Map<String,Object>>> lastOilInfo=new ListUtil().groupBy("PfVehicleid", listOil);
		 
		List<String>vclList=new ArrayList<String>();
		if(list!=null&&list.size()>0){
			for(Map<String,Object>map:list){
				String vclId=MapUtils.getString(map,"PfVehicleid");
				vclList.add(VclPreFiex+vclId);
			}
		}
		//没有设备直接退出
		if(vclList.size()==0){
			return null;
		}
		//获取查询参数
		List<String>dutyNumber=new ArrayList<String>();
		dutyNumber.add(EngineWorkTime);
		dutyNumber.add(EngineIdeaOil);
		dutyNumber.add(EngineIdeaWorkTime);
		dutyNumber.add(EngineOil);
		String beginTime=date+" 00:00:00";
		String endTime=DateRegUtil.addDay(date, 1)+" 00:00:00";
		//查询数据范围
		try {
			Map<String, Map<String, List<Point>>> mapResult=Kmxclient.getReader().getRange(vclList, dutyNumber,DateRegUtil.GetTimes(beginTime), DateRegUtil.GetTimes(endTime));
			List<String>listsql=new ArrayList<String>(); 
			if(MapUtils.isNotEmpty(mapResult)){
				for(String VclId:mapResult.keySet()){//设备ID
					Map<String,Object>mapDutyA=new HashMap<String,Object>();
					mapDutyA.put(EngineWorkTime,"1");
					mapDutyA.put(EngineIdeaOil,"1");
					mapDutyA.put(EngineIdeaWorkTime,"1");
					mapDutyA.put(EngineOil,"1");
					//从设备基本信息中清除掉
					mapInfo.remove(VclId.substring(4,VclId.length()));
					Map<String, List<Point>>mapAllDutyValue=mapResult.get(VclId);
					Map<String,Object>mapParam=new HashMap<String, Object>();
					
					for(String dutyKey:mapAllDutyValue.keySet()){//工况编号
						List<Point>listResult=mapAllDutyValue.get(dutyKey);
						//第一条值
						String firstV=null;
						//最后一条值条值
						String lastEndV=null;
						//获取第一条
						for(int i=0;i<listResult.size();i++){
							Point point=listResult.get(i);
							String value=point.getValue();
							if(StringUtils.isNoneBlank(value)){
								firstV=value;
								break;
							}
							
						}
						for(int i=listResult.size()-1;i>=0;i--){
							Point point=listResult.get(i);
							String value=point.getValue();
							if(StringUtils.isNoneBlank(value)){
								lastEndV=value;
								break;
							}
							
						}
						
						if(EngineWorkTime.equals(dutyKey)){	//工作时长处理
							mapDutyA.remove(dutyKey);
							if(StringUtils.isBlank(lastEndV)){
								List<Map<String,Object>>lastOilInfoTheVcl=lastOilInfo.get(VclId.substring(4,VclId.length()));
								if(lastOilInfoTheVcl!=null&&lastOilInfoTheVcl.size()>0){
									lastEndV=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tWork");
								}
							}
							if(StringUtils.isBlank(firstV)||StringUtils.isBlank(lastEndV)){
								mapParam.put("MsgESS_iWork",null);
							}else{
								mapParam.put("MsgESS_iWork",Double.valueOf(lastEndV)-Double.valueOf(firstV));
							}
							mapParam.put("MsgESS_tWork", lastEndV);
						}else if(EngineIdeaOil.equals(dutyKey)){//怠速油耗处理
							mapDutyA.remove(dutyKey);
							if(StringUtils.isBlank(lastEndV)){
								List<Map<String,Object>>lastOilInfoTheVcl=lastOilInfo.get(VclId.substring(4,VclId.length()));
								if(lastOilInfoTheVcl!=null&&lastOilInfoTheVcl.size()>0){
									lastEndV=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tIdleWorkTime_PGN");
								}
							}
							if(StringUtils.isBlank(firstV)||StringUtils.isBlank(lastEndV)){
								mapParam.put("MsgESS_iIdleWorkTime_PGN",null);
							}else{
								mapParam.put("MsgESS_iIdleWorkTime_PGN",Double.valueOf(lastEndV)-Double.valueOf(firstV));
							}
							mapParam.put("MsgESS_tIdleWorkTime_PGN", lastEndV);
						}else if(EngineIdeaWorkTime.equals(dutyKey)){//怠速时长处理
							mapDutyA.remove(dutyKey);
							if(StringUtils.isBlank(lastEndV)){
								List<Map<String,Object>>lastOilInfoTheVcl=lastOilInfo.get(VclId.substring(4,VclId.length()));
								if(lastOilInfoTheVcl!=null&&lastOilInfoTheVcl.size()>0){
									lastEndV=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tIdleWorkTime_UDS");
								}
							}
							if(StringUtils.isBlank(firstV)||StringUtils.isBlank(lastEndV)){
								mapParam.put("MsgESS_iIdleWorkTime_UDS",null);
							}else{
								mapParam.put("MsgESS_iIdleWorkTime_UDS",Double.valueOf(lastEndV)-Double.valueOf(firstV));
							}
							mapParam.put("MsgESS_tIdleWorkTime_UDS", lastEndV);
						}else if(EngineOil.equals(dutyKey)){//油耗处理
							mapDutyA.remove(dutyKey);
							if(StringUtils.isBlank(lastEndV)){
								List<Map<String,Object>>lastOilInfoTheVcl=lastOilInfo.get(VclId.substring(4,VclId.length()));
								if(lastOilInfoTheVcl!=null&&lastOilInfoTheVcl.size()>0){
									lastEndV=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tOilCons");
								}
							}
							if(StringUtils.isBlank(firstV)||StringUtils.isBlank(lastEndV)){
								mapParam.put("MsgESS_iOilCons",null);
							}else{
								mapParam.put("MsgESS_iOilCons",Double.valueOf(lastEndV)-Double.valueOf(firstV));
							}
							mapParam.put("MsgESS_tOilCons", lastEndV);
						}
					}
					if(mapDutyA.size()>0){
						for(String key:mapDutyA.keySet()){
							if(EngineWorkTime.equals(key)){
								String endValue=null;
								List<Map<String,Object>>lastOilInfoTheVcl=lastOilInfo.get(VclId.substring(4,VclId.length()));
								if(lastOilInfoTheVcl!=null&&lastOilInfoTheVcl.size()>0){
									endValue=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tWork");
								}
								mapParam.put("MsgESS_iWork",null);
								mapParam.put("MsgESS_tWork",endValue);
							}else if(EngineIdeaOil.equals(key)){
								String endValue=null;
								List<Map<String,Object>>lastOilInfoTheVcl=lastOilInfo.get(VclId.substring(4,VclId.length()));
								if(lastOilInfoTheVcl!=null&&lastOilInfoTheVcl.size()>0){
									endValue=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_iIdleWorkTime_PGN");
								}
								mapParam.put("MsgESS_iIdleWorkTime_PGN",null);
								mapParam.put("MsgESS_tIdleWorkTime_PGN",endValue);
							}else if(EngineIdeaWorkTime.equals(key)){
								String endValue=null;
								List<Map<String,Object>>lastOilInfoTheVcl=lastOilInfo.get(VclId.substring(4,VclId.length()));
								if(lastOilInfoTheVcl!=null&&lastOilInfoTheVcl.size()>0){
									endValue=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tIdleWorkTime_UDS");
								}
								mapParam.put("MsgESS_iIdleWorkTime_UDS",null);
								mapParam.put("MsgESS_tIdleWorkTime_UDS",endValue);
							
							}else if(EngineOil.equals(key)){
								String endValue=null;
								List<Map<String,Object>>lastOilInfoTheVcl=lastOilInfo.get(VclId.substring(4,VclId.length()));
								if(lastOilInfoTheVcl!=null&&lastOilInfoTheVcl.size()>0){
									endValue=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tOilCons");
								}
								mapParam.put("MsgESS_iOilCons",null);
								mapParam.put("MsgESS_tOilCons",endValue);
							}
							
						}
					}
					if(mapParam.size()>0){
						String execdate=date.replaceAll("-","").substring(0,6);
						String sql="insert into Msg_EquipmentState_Statistics_"+execdate+"(MsgESS_VclID,MsgESS_kmsdeviceId,MsgESS_StatisticsTime,MsgEss_uniqueness,";
						String insertvalue=")values ("+VclId.substring(4,VclId.length())+",'"+VclId+"','"+date+"','"+VclId.substring(4,VclId.length())+"-"+date+"',";
						String updatevalue=" ON DUPLICATE KEY UPDATE ";
						for(String keyData:mapParam.keySet()){
							sql+=keyData+",";
							insertvalue+=MapUtils.getString(mapParam, keyData)+",";
							updatevalue+=keyData+"="+MapUtils.getString(mapParam, keyData)+",";
						}
						sql=sql.substring(0,sql.length()-1)+insertvalue.substring(0,insertvalue.length()-1)+")"+updatevalue.substring(0,updatevalue.length()-1);
						System.out.println(sql);
						listsql.add(sql);
					}
				}
			}
			if(mapInfo.size()>0){//未插入的重新插入
				String execdate=date.replaceAll("-","").substring(0,6);
				for(String key:mapInfo.keySet()){
					List<Map<String,Object>>lastOilInfoTheVcl=lastOilInfo.get(key);
					String MsgESS_tWork=null;
					String MsgESS_tOilCons=null;
					String MsgESS_tIdleWorkTime_PGN=null;
					String MsgESS_tIdleWorkTime_UDS=null;
					if(lastOilInfoTheVcl!=null&&lastOilInfoTheVcl.size()>0){
						MsgESS_tWork=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tWork");
						MsgESS_tOilCons=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tOilCons");
						MsgESS_tIdleWorkTime_PGN=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tIdleWorkTime_PGN");
						MsgESS_tIdleWorkTime_UDS=MapUtils.getString(lastOilInfoTheVcl.get(0),"MsgESS_tIdleWorkTime_UDS");
					}
					String sql=" insert into Msg_EquipmentState_Statistics_"+execdate+"(MsgESS_VclID,MsgESS_kmsdeviceId,"
							+ " MsgESS_StatisticsTime,MsgEss_uniqueness,MsgESS_iWork,MsgESS_tWork,MsgESS_iOilCons,"
							+ " MsgESS_tOilCons,MsgESS_iIdleWorkTime_PGN,MsgESS_tIdleWorkTime_PGN,MsgESS_iIdleWorkTime_UDS,MsgESS_tIdleWorkTime_UDS)values "
							+ " ("+key+",'"+VclPreFiex+key+"','"+date+"','"+key+"-"+date+"',null,"+MsgESS_tWork+",null,"+MsgESS_tOilCons+",null,"
									+ ""+MsgESS_tIdleWorkTime_PGN+",null,"+MsgESS_tIdleWorkTime_UDS+") ON DUPLICATE KEY UPDATE "
											+ "MsgESS_iWork=null,MsgESS_tWork="+MsgESS_tWork+",MsgESS_iOilCons=null,MsgESS_tOilCons="+MsgESS_tOilCons+","
													+ "MsgESS_iIdleWorkTime_PGN=null,MsgESS_tIdleWorkTime_PGN="+MsgESS_tIdleWorkTime_PGN+","
															+ "MsgESS_iIdleWorkTime_UDS=null,MsgESS_tIdleWorkTime_UDS="+MsgESS_tIdleWorkTime_UDS;
					listsql.add(sql);
				}
			}
			XxlJobLogger.log("车天本次需要执行："+listsql.size()+"条");
			if(listsql.size()>0){
				String obj[]=new String[listsql.size()];
				int i=0;
				for(String sql:listsql){
					obj[i++]=sql;
				}
				vclDayDao.execBatch(obj);
			}
		} catch (NoHostAvailableException | QueryExecutionException | TimeoutException | NoMetricException
				| NotImplementedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
