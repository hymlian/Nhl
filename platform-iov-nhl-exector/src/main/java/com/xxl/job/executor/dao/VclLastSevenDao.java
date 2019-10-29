package com.xxl.job.executor.dao;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.xxl.job.executor.util.DateRegUtil;

@Component
public class VclLastSevenDao {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	/**查询对应时区的设备
	 * @param condition
	 * @return
	 */
	public List<Map<String,Object>> getExecVcl(String condition,String bgndate,String enddate){
		//根据开始时间和结束时间判断月份
		String monthArray[]=DateRegUtil.getMonthSql(bgndate,enddate);
		String monthsql="";
		for(String month:monthArray){
			monthsql+=" select msgess_vclid,MsgESS_iWork,MsgESS_iOilCons,MsgESS_iIdleWorkTime_PGN,MsgESS_iIdleWorkTime_UDS from Msg_EquipmentState_Statistics_"+month+
					   " where MsgESS_StatisticsTime<='"+enddate+"' and MsgESS_StatisticsTime>='"+bgndate+"' union all ";
		}
		monthsql=monthsql.substring(0,monthsql.length()-10);
		String sql=	  "SELECT  msgess_vclid,"+
					  "COUNT(msgess_vclid)count,"+
					  "SUM(MsgESS_iWork) MsgESS_iWork,"+
					  "SUM(MsgESS_iOilCons) MsgESS_iOilCons,"+
					  "SUM(MsgESS_iIdleWorkTime_PGN) MsgESS_iIdleWorkTime_PGN,"+
					  "SUM(MsgESS_iIdleWorkTime_UDS) MsgESS_iIdleWorkTime_UDS "+
					  "from vehicleinfo INNER JOIN TimeZone ON TZ_ID = TimeZoneID "+
					  "INNER JOIN ("+monthsql+")month ON PfVehicleid = msgess_vclid WHERE PFVehicleId IS NOT NULL ";
		if(StringUtils.isNotBlank(condition)){
			sql+=" and "+condition;
		}
		sql+=" group by msgess_vclid";
		return jdbcTemplate.queryForList(sql);
	}
	
	/**查询对应时区的设备
	 * @param condition
	 * @return
	 */
	public List<Map<String,Object>> getLastDayVclOilInfo(String condition,String date){
		String execDate=DateRegUtil.addDay(date, -1);
		String execDateSub=execDate.replaceAll("-","");
		String sql="SELECT PfVehicleid,"+
			      "MsgESS_tOilCons,MsgESS_tIdleWorkTime_PGN,MsgESS_tIdleWorkTime_UDS,MsgESS_tWork "+
			" FROM   vehicleinfo INNER JOIN TimeZone ON TZ_ID=TimeZoneID inner join Msg_EquipmentState_Statistics_"+execDateSub.substring(0,6)+" on PfVehicleid=Msgess_VclID and MsgESS_StatisticsTime='"+execDate+"'  where 1=1 and PfVehicleid is not null";
		if(StringUtils.isNotBlank(condition)){
			sql+=" and "+condition;
		}
		
		return jdbcTemplate.queryForList(sql);
	}

	/**
	 * 批量执行
	 * @param obj
	 * @return
	 */
	public int [] execBatch(String []sqlString){
		return jdbcTemplate.batchUpdate(sqlString);
	}
}
