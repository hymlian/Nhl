package com.xxl.job.executor.service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.dao.VclMonthDao;

import tsinghua.thss.sdk.core.Client;

@Component
public class VclMonthService {
	@Autowired
	private VclMonthDao vclMonthDao;
	
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
		//判断当月还是上月
		String execdate=date.replaceAll("-","").substring(0,6);
		//获取对应时区的设备自然月的值
		List<Map<String,Object>>list=vclMonthDao.getExecVcl(condition,date);
		List<String>listsql=new ArrayList<String>();
		//查询数据范围
		if(list.size()>0){
			for(Map<String,Object>mapResult:list){
				String MsgESM_VclID=MapUtils.getString(mapResult,"msgess_vclid");
				String MsgESM_WorkDay=MapUtils.getString(mapResult,"count");
				String MsgESM_iWork=MapUtils.getString(mapResult,"MsgESS_iWork");
				
				String MsgESM_iOilCons=MapUtils.getString(mapResult,"MsgESS_iOilCons");
				
				String MsgESM_iIdleWorkTime_PGN=MapUtils.getString(mapResult,"MsgESS_iIdleWorkTime_PGN");
				
				String MsgESM_iIdleWorkTime_UDS=MapUtils.getString(mapResult,"MsgESS_iIdleWorkTime_UDS");
				
				String sql="insert into Msg_EquipmentState_Statistics_MonthJH_"+execdate+"(MsgESM_VclID,MsgESM_WorkDay,MsgESM_iWork,MsgESM_iOilCons,MsgESM_iIdleWorkTime_PGN,MsgESM_iIdleWorkTime_UDS) "+ 
						"Values ("+MsgESM_VclID+","+MsgESM_WorkDay+","+MsgESM_iWork+","+MsgESM_iOilCons+","+MsgESM_iIdleWorkTime_PGN+","+MsgESM_iIdleWorkTime_UDS+") "+
								"ON DUPLICATE KEY UPDATE MsgESM_WorkDay="+MsgESM_WorkDay+",MsgESM_iWork="+MsgESM_iWork+",MsgESM_iOilCons="+MsgESM_iOilCons+","+
										 "MsgESM_iIdleWorkTime_PGN="+MsgESM_iIdleWorkTime_PGN+",MsgESM_iIdleWorkTime_UDS="+MsgESM_iIdleWorkTime_UDS;
				listsql.add(sql);
			}
			XxlJobLogger.log("本次执行月度聚合条数："+listsql.size()+"条");
			String []strSql=new String[listsql.size()];
			int i=0;
			for(String sql:listsql){
				strSql[i++]=sql;
			}
			vclMonthDao.execBatch(strSql);
			
		}
		return null;
	}
}
