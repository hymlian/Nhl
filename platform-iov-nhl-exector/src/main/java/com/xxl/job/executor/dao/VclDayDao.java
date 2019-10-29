package com.xxl.job.executor.dao;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.xxl.job.executor.util.DateRegUtil;

@Component
public class VclDayDao {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	/**查询对应时区的设备
	 * @param condition
	 * @return
	 */
	public List<Map<String,Object>> getExecVcl(String condition){
		String sql="SELECT PfVehicleid,"+
			      " TZ_Type,TZ_DeffHour "+
			" FROM   vehicleinfo INNER JOIN TimeZone ON TZ_ID=TimeZoneID where 1=1 and PfVehicleid is not null";
		if(StringUtils.isNotBlank(condition)){
			sql+=" and "+condition;
		}
		
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
			" FROM   vehicleinfo INNER JOIN TimeZone ON TZ_ID=TimeZoneID inner join Msg_EquipmentState_Statistics_"+execDateSub.substring(0,6)+" on PfVehicleid=Msgess_VclID and MsgESS_StatisticsTime='"+execDate+"'  where PfVehicleid is not null ";
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
