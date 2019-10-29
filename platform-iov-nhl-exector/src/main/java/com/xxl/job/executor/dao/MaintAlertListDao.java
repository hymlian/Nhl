package com.xxl.job.executor.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class MaintAlertListDao {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	public List<Map<String,Object>> getmesList(String oneDay,String biao){
		List<Map<String,Object>> mes = new ArrayList<Map<String,Object>>();
		String  sql="SELECT mes.MsgESS_VclID,mes.MsgESS_tWork,vo.VehicleNumber,vo.VehicleTypeId"
				+" FROM "
				+"msg_equipmentstate_statistics_"+biao+" mes "
				+"LEFT JOIN vehicleinfo vo ON vo.PFVehicleId = mes.MsgESS_VclID"
				+" WHERE "
				+"mes.MsgESS_StatisticsTime ='"+oneDay+"'";
		try{
			mes=jdbcTemplate.queryForList(sql);
		}catch(Exception e){
			e.printStackTrace();
		}
		return mes;
	}
	
	public Map<String,Object> getmaintrulerList(int vehicleTypeId){
		Map<String,Object> mes = new HashMap();
		String  sql="select MR_ID,MR_AdvanceHour from maintruler where MR_VehicleTypeID='"+vehicleTypeId+"'";
		try{
			mes=jdbcTemplate.queryForMap(sql);
		}catch(Exception e){
			e.printStackTrace();
		}
		return mes;
	}
	
	public List<Map<String,Object>> maintrulerdetailList(int mrId){
		List<Map<String,Object>> mes = new ArrayList<Map<String,Object>>();
		String  sql="select * from maintrulerdetail where MRD_MR_ID='"+mrId+"'";
		try{
			mes=jdbcTemplate.queryForList(sql);
		}catch(Exception e){
			e.printStackTrace();
		}
		return mes;
	}
	
	
	public List<Map<String,Object>> maintalertList(){
		List<Map<String,Object>> mes = new ArrayList<Map<String,Object>>();
		String  sql="select MA_VCLID,MA_Content,MAX(MA_AlertDate) MA_AlertDate,MA_Component,MA_AlertWorkHour from maintalert group by MA_VCLID";
		try{
			mes=jdbcTemplate.queryForList(sql);
		}catch(Exception e){
			e.printStackTrace();
		}
		return mes;
	}
}
