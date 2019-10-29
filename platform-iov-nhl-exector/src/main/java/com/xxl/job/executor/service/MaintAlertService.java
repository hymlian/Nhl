package com.xxl.job.executor.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import com.xxl.job.executor.dao.MaintAlertDao;

import net.tycmc.bulb.common.util.DateUtil;


@Component
public class MaintAlertService {
	@Autowired
	private MaintAlertDao maintAlertDao;
	
	/**
	 * 获取每台车最新一条保养提醒
	 * @return
	 */
	public List<Map<String,Object>> getLastMaintAlertPerVcl(Map<String,Object>param){
		return maintAlertDao.getLastMaintAlertPerVcl(param);
	}
	/**
	 * 获取每台设备昨天的累积工作时长
	 * @return
	 */
	public List<Map<String,Object>> getLJWorkHourPerVcl(String dateStr){
		return maintAlertDao.getLJWorkHourPerVcl(dateStr);
	}
	/**
	 * 获取每台车不同规则的保养提醒信息
	 * @return
	 */
	public List<Map<String,Object>> getMaintInfoPerVclDiffRuler(){
		return maintAlertDao.getMaintInfoPerVclDiffRuler();
	}
	/**
	 * 获取每台车不同规则的保养提醒信息
	 * @return
	 */
	public List<Map<String,Object>> getLast30DayAvageWorkHour(){
		List<Map<String,Object>>list= maintAlertDao.getLast30DayAvageWorkHour();
		if(list.size()>0){
			for(Map<String,Object>mapWorkHour:list){//工作时长
				double MsgESSLT_iWork=MapUtils.getDoubleValue(mapWorkHour, "MsgESSLT_iWork");
				int numDay=MapUtils.getIntValue(mapWorkHour, "numDay");
				MsgESSLT_iWork=MsgESSLT_iWork/numDay;
				//计算近30天的平局值，并保留2位小数，四舍五入
				BigDecimal bd=new BigDecimal(MsgESSLT_iWork);
				BigDecimal result=bd.setScale(2,RoundingMode.HALF_UP);
				double avgwh=result.doubleValue();
				mapWorkHour.put("averageWH",avgwh);
			}
		}
		return list;
	}
	/**
	 * 获取每台车不同规则的保养提醒信息
	 * @return
	 */
	public boolean execBatch(List<String>sqlList){
		int[] result= maintAlertDao.execBatch(sqlList);
		if(result!=null&&result.length>0){
			for(int resultFlag:result){
				if(resultFlag==0){
					return false;
				}
			}
			return true;
		}
		return false;
	}
	/**
	 * 执行保养提醒的逻辑
	 * @param mapresult：统计的设备基本信息
	 * @param vclWorkHour：统计的设备油耗信息
	 * @param vclPerRuler：统计的设备各个规则下，最新一条保养提醒信息
	 * @return
	 */
	@Async
	public Future<String> execMaintAlertService(Map<String,Object>mapresult,List<Map<String,Object>>vclWorkHour,List<Map<String,Object>>vclPerRuler){
			if(vclWorkHour==null||vclWorkHour.size()==0){
				return null;
			}
			if(vclPerRuler==null||vclPerRuler.size()==0){
				return null;
			}
			//获取此设备累积工作时长
			double workHour=MapUtils.getDoubleValue(vclWorkHour.get(0),"MsgESS_tWork");
			if(workHour==0){
				return null;
			}
			String vehicleId=MapUtils.getString(mapresult,"PfVehicleid");
			String maintContent="";
			String maintComponent="";
			String maintRulerId="";
			String alertDate=MapUtils.getString(mapresult,"alertDate");
			double VclnextMaintWorkHour=0;
			//获取下次工作时长
 			for(Map<String,Object>mapPerRuler:vclPerRuler){
				double mri_workHour=MapUtils.getDoubleValue(mapPerRuler,"mri_workHour");//本次保养对应的保养记录的工作时长，如果没有保养过，则为0
				double MRD_PerHour=MapUtils.getDoubleValue(mapPerRuler,"MRD_PerHour");//本次保养对应的间隔时长
				int MR_AdvanceHour=MapUtils.getIntValue(mapPerRuler,"MR_AdvanceHour");//提前提醒小时数
				String mrd_id=MapUtils.getString(mapPerRuler,"mrd_id");//保养规则ID
				int nextNum=(int)((workHour-mri_workHour)/MRD_PerHour);
				if(nextNum<0){   
					nextNum=0;
				}
				nextNum++;
				double VclnextMaintWorkHour1=nextNum*MRD_PerHour+mri_workHour;
				String MRD_Component=MapUtils.getString(mapPerRuler,"MRD_Component");
				if((MR_AdvanceHour+workHour)>=VclnextMaintWorkHour1){//应该保养
					if(VclnextMaintWorkHour==0){
						VclnextMaintWorkHour=VclnextMaintWorkHour1;
					}else{//比较大小，取小的那个
						if(VclnextMaintWorkHour>VclnextMaintWorkHour1){
							VclnextMaintWorkHour=VclnextMaintWorkHour1;
						}
					}
					maintContent+=","+MRD_PerHour+"小时";
					maintRulerId+=","+mrd_id;
					if(StringUtils.isNotBlank(MRD_Component)){
						maintComponent+=","+MRD_Component;
					}
					//maintContent+=","+MRD_PerHour;
				}
			}
			if(StringUtils.isNotBlank(maintContent)&&StringUtils.isNotBlank(maintRulerId)){//产生提醒
				maintContent=maintContent.substring(1,maintContent.length());
				maintComponent=maintComponent.substring(1,maintComponent.length());
				maintRulerId=maintRulerId.substring(1,maintRulerId.length());
				String sql="insert into maintAlert(MA_VCLID,MA_Content,MA_AlertDate,MA_Component,MA_MRD_ID,MA_AlertWorkHour)values('"+vehicleId+"','"+maintContent+"','"+alertDate+"','"+maintComponent+"','"+maintRulerId+"',"+VclnextMaintWorkHour+")";
 				return new AsyncResult<>(sql);
			}
		return null;
	}
	/**
	 * 执行保养预测的逻辑
	 * @param mapresult：统计的设备基本信息
	 * @param vclWorkHour：统计的设备油耗信息
	 * @param vclPerRuler：统计的设备各个规则下，最新一条保养提醒信息
	 * @return
	 */
	@Async
	public Future<String> execMaintForcastService(Map<String,Object>mapresult,List<Map<String,Object>>vclWorkHour,List<Map<String,Object>>vclPerRuler,List<Map<String,Object>>AvgWorkHour){
 			if(vclWorkHour==null||vclWorkHour.size()==0){
				return null;
			}
			if(vclPerRuler==null||vclPerRuler.size()==0){
				return null;
			}
			if(AvgWorkHour==null||AvgWorkHour.size()==0){
				return null;
			}
			//获取此设备累积工作时长
			double workHour=MapUtils.getDoubleValue(vclWorkHour.get(0),"MsgESS_tWork");
			if(workHour==0){
				return null;
			}
			//获取此设备累积工作时长
			double averageWH=MapUtils.getDoubleValue(AvgWorkHour.get(0),"averageWH");
			if(averageWH==0||averageWH>24){
				return null;
			}
			String vehicleId=MapUtils.getString(mapresult,"PfVehicleid");
			String dateStr=MapUtils.getString(mapresult,"dateStr");
			int needDay=-1;
			double ForcastworkHour=0;
			//获取下次工作时长
 			for(Map<String,Object>mapPerRuler:vclPerRuler){
				double mri_workHour=MapUtils.getDoubleValue(mapPerRuler,"mri_workHour");//本次保养对应的保养记录的工作时长，如果没有保养过，则为0
				double MRD_PerHour=MapUtils.getDoubleValue(mapPerRuler,"MRD_PerHour");//本次保养对应的间隔时长
				int nextNum=(int)((workHour-mri_workHour)/MRD_PerHour);
				if(nextNum<0){
					nextNum=0;
				}
				nextNum++;
				double VclnextMaintWorkHour=nextNum*MRD_PerHour+mri_workHour;
				//间隔的工作时长
				double deffWorkHour=VclnextMaintWorkHour-workHour;
				//此差距所需天书
				int day=0;
				if(deffWorkHour%averageWH==0){
					day=(int)(deffWorkHour/averageWH);
				}else{
					day=(int)(deffWorkHour/averageWH)+1;
				}
				
				if(needDay==-1){
					needDay=day;
					ForcastworkHour=VclnextMaintWorkHour;
				}else if(needDay>day){
					needDay=day;
					ForcastworkHour=VclnextMaintWorkHour;
				}
			}
			if(needDay>-1){//产生预测
				String forcastDate=DateUtil.addDay(dateStr, needDay);
				String sql="insert into maintForeCast(MFC_VCLID,MFC_ForeCaseDate,MFC_Content)values('"+vehicleId+"','"+forcastDate+"','"+ForcastworkHour+"小时') on duplicate key update MFC_ForeCaseDate='"+forcastDate+"',MFC_Content='"+ForcastworkHour+"小时'";
				return new AsyncResult<>(sql);
			}
		return null;
	}
}
