package com.xxl.job.executor.service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.dao.VclLastSevenDao;


@Component
public class VclLastSevenService {
	@Autowired
	private VclLastSevenDao vclLastSevenDao;
	
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
	public List<Map<String,Object>> getExecVcl(String condition,String BgnDate,String EndDate,String tableName){
		//获取对应时区的设备自然月的值
		List<Map<String,Object>>list=vclLastSevenDao.getExecVcl(condition,BgnDate,EndDate);
		List<String>listsql=new ArrayList<String>();
		//查询数据范围
		if(list.size()>0){
			for(Map<String,Object>mapResult:list){
				String MsgESSLS_VclID=MapUtils.getString(mapResult,"msgess_vclid");
				String MsgESSLS_WorkDay=MapUtils.getString(mapResult,"count");
				String MsgESSLS_iWork=MapUtils.getString(mapResult,"MsgESS_iWork");
				
				String MsgESSLS_iOilCons=MapUtils.getString(mapResult,"MsgESS_iOilCons");
				
				String MsgESSLS_iIdleWorkTime_PGN=MapUtils.getString(mapResult,"MsgESS_iIdleWorkTime_PGN");
				
				String MsgESSLS_iIdleWorkTime_UDS=MapUtils.getString(mapResult,"MsgESS_iIdleWorkTime_UDS");
				//Msg_EquipmentState_Statistics_LastSeven
				String sql="insert into "+tableName+" (MsgESSLS_VclID,MsgESSLS_WorkDay,MsgESSLS_iWork,MsgESSLS_iOilCons,MsgESSLS_iIdleWorkTime_PGN,MsgESSLS_iIdleWorkTime_UDS) "+ 
						"Values ("+MsgESSLS_VclID+","+MsgESSLS_WorkDay+","+MsgESSLS_iWork+","+MsgESSLS_iOilCons+","+MsgESSLS_iIdleWorkTime_PGN+","+MsgESSLS_iIdleWorkTime_UDS+") "+
								"ON DUPLICATE KEY UPDATE MsgESSLS_WorkDay="+MsgESSLS_WorkDay+",MsgESSLS_iWork="+MsgESSLS_iWork+",MsgESSLS_iOilCons="+MsgESSLS_iOilCons+","+
										 "MsgESSLS_iIdleWorkTime_PGN="+MsgESSLS_iIdleWorkTime_PGN+",MsgESSLS_iIdleWorkTime_UDS="+MsgESSLS_iIdleWorkTime_UDS;
				listsql.add(sql);
			}
			XxlJobLogger.log("本次执行近七天条数："+listsql.size()+"条");
			String []strSql=new String[listsql.size()];
			int i=0;
			for(String sql:listsql){
				strSql[i++]=sql;
			}
			vclLastSevenDao.execBatch(strSql);
			
		}
		return null;
	}
	
	/**
	 * @param condition
	 * @return
	 * @throws ParseException 
	 */
	public List<Map<String,Object>> getLastThirtyExecVcl(String condition,String BgnDate,String EndDate,String tableName){
		//获取对应时区的设备自然月的值
		List<Map<String,Object>>list=vclLastSevenDao.getExecVcl(condition,BgnDate,EndDate);
		List<String>listsql=new ArrayList<String>();
		//查询数据范围
		if(list.size()>0){
			for(Map<String,Object>mapResult:list){
				String MsgESSLT_VclID=MapUtils.getString(mapResult,"msgess_vclid");
				String MsgESSLT_WorkDay=MapUtils.getString(mapResult,"count");
				String MsgESSLT_iWork=MapUtils.getString(mapResult,"MsgESS_iWork");
				
				String MsgESSLT_iOilCons=MapUtils.getString(mapResult,"MsgESS_iOilCons");
				
				String MsgESSLT_iIdleWorkTime_PGN=MapUtils.getString(mapResult,"MsgESS_iIdleWorkTime_PGN");
				
				String MsgESSLT_iIdleWorkTime_UDS=MapUtils.getString(mapResult,"MsgESS_iIdleWorkTime_UDS");
				//Msg_EquipmentState_Statistics_LastSeven
				String sql="insert into "+tableName+" (MsgESSLT_VclID,MsgESSLT_WorkDay,MsgESSLT_iWork,MsgESSLT_iOilCons,MsgESSLT_iIdleWorkTime_PGN,MsgESSLT_iIdleWorkTime_UDS) "+ 
						"Values ("+MsgESSLT_VclID+","+MsgESSLT_WorkDay+","+MsgESSLT_iWork+","+MsgESSLT_iOilCons+","+MsgESSLT_iIdleWorkTime_PGN+","+MsgESSLT_iIdleWorkTime_UDS+") "+
								"ON DUPLICATE KEY UPDATE MsgESSLT_WorkDay="+MsgESSLT_WorkDay+",MsgESSLT_iWork="+MsgESSLT_iWork+",MsgESSLT_iOilCons="+MsgESSLT_iOilCons+","+
										 "MsgESSLT_iIdleWorkTime_PGN="+MsgESSLT_iIdleWorkTime_PGN+",MsgESSLT_iIdleWorkTime_UDS="+MsgESSLT_iIdleWorkTime_UDS;
				listsql.add(sql);
			}
			XxlJobLogger.log("本次执行近30天条数："+listsql.size()+"条");
			String []strSql=new String[listsql.size()];
			int i=0;
			for(String sql:listsql){
				strSql[i++]=sql;
			}
			vclLastSevenDao.execBatch(strSql);
			
		}
		return null;
	}
}
