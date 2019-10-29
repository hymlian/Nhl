package com.xxl.job.executor.dao;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.xxl.job.executor.util.DateRegUtil;

import net.tycmc.bulb.common.util.DateUtil;

@Component
public class MaintAlertDao  {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	/**
	 * 获取每台车最新一条保养提醒
	 * @return
	 */
	public List<Map<String,Object>> getLastMaintAlertPerVcl(Map<String,Object>param){
		String sql="SELECT PfVehicleid,ma_vclid,"+
				      " mri_ma_id,TZ_Type,TZ_DeffHour "+
				" FROM   vehicleinfo INNER JOIN TimeZone ON TZ_ID=TimeZoneID "+
				       " left JOIN (SELECT   ma_vclid, "+
				                           " Max(Concat(ma_alertdate,',',ma_id)) aa "+
				                  " FROM     maintalert "+
				                  " GROUP BY ma_vclid) maint "+
				        " ON pfvehicleid = maint.ma_vclid "+
				       "LEFT JOIN maintrecordinfo "+
				         "ON Substr(maint.aa,12,Length(maint.aa)) = maintrecordinfo.mri_ma_id  WHERE pfvehicleid IS NOT NULL";
		String condition=MapUtils.getString(param,"condition");
		if(StringUtils.isNotBlank(condition)){
			sql+=" and "+condition;
		}
		return jdbcTemplate.queryForList(sql);
	}
	
	/**
	 * 获取每台设备昨天的累积工作时长
	 * @return
	 */
	public List<Map<String,Object>> getLJWorkHourPerVcl(String dateStr){
		String dateStrS=dateStr.replaceAll("-","").substring(0,6);
		//日表中没有数据的设备，累积工作时长按照0处理
 		String sql="select PfVehicleId,IFNULL(MsgESS_tWork,0)MsgESS_tWork  from VehicleInfo inner join msg_equipmentstate_statistics_"+dateStrS+" ON Pfvehicleid=Msgess_vclid "
				+ "WHERE PFVehicleId IS NOT NULL And MsgESS_StatisticsTime='"+dateStr+"'";
		return jdbcTemplate.queryForList(sql);
	}
	/**
	 * 获取每台车不同规则的保养提醒信息
	 * @return
	 */
	public List<Map<String,Object>> getMaintInfoPerVclDiffRuler(){
		String sql="SELECT   pfvehicleid,"+
		         "ma_rulerid,"+
		         "maintrulerdetail.mrd_id,"+
		         "IFNULL(mri_workhour,0)mri_workhour,"+
		         "vehicletypeid,"+
		         "MR_AdvanceHour,"+
		         "MRD_PerHour,"+
		         "MRD_Component "+
		"FROM     vehicleinfo "+
		         "INNER JOIN maintruler "+
		           "ON vehicletypeid = mr_vehicletypeid "+
		         "INNER JOIN maintrulerdetail "+
		           "ON mr_id = mrd_mr_id "+
		         "LEFT JOIN (SELECT bb.ma_rulerid,vm.ma_vclid,"+
		                           "mri_workhour "+
		                    "FROM   maintalert vm "+
		                           "INNER JOIN (SELECT  ma_vclid, ma_rulerid,"+
		                                                "MAX(CONCAT(ma_alertdate,',',ma_id)) aa "+
		                                       "FROM     v_maintalert "+
		                                       "GROUP BY ma_vclid,"+
		                                                "ma_rulerid) bb "+
		                             "ON vm.ma_id = SUBSTR(bb.aa,12,LENGTH(bb.aa)) "+
		                           "LEFT JOIN maintrecordinfo "+
		                             "ON ma_id = mri_ma_id) maintinfo "+
		           " ON maintinfo.ma_vclid=pfvehicleid  "+
		"WHERE    pfvehicleid IS NOT NULL and  (ma_rulerid IS NULL OR ma_rulerid =mrd_id) "+
		"ORDER BY pfvehicleid";
		return jdbcTemplate.queryForList(sql);
	}
	/**
	 * 获取近30天的平均油耗
	 * @return
	 */
	public List<Map<String,Object>> getLast30DayAvageWorkHour(){
		String dateStrEnd=DateUtil.addDay(-1);
		String dateStrBegin=DateUtil.addDay(-30);
		String []monthArr=DateRegUtil.getMonthSql(dateStrBegin,dateStrEnd);
		StringBuffer sbf=new StringBuffer();
		for(String monthItem:monthArr){ 
			sbf.append("SELECT Msgess_VclId MsgESSLT_VclID,IFNULL(Msgess_iwork,0)MsgESSLT_iWork FROM Msg_EquipmentState_Statistics_"+monthItem+" where msgess_statisticsTime<='"+dateStrEnd+"' AND msgess_statisticsTime>='"+dateStrBegin+"' union all " );
		}
		String sqlResult=sbf.toString().substring(0,sbf.toString().length()-10);
		sqlResult="select MsgESSLT_VclID,sum(MsgESSLT_iWork)MsgESSLT_iWork,count(MsgESSLT_VclID)numDay from ("+sqlResult+")AA group by MsgESSLT_VclID";
		return jdbcTemplate.queryForList(sqlResult);
	}
	
	/**
	 * 批量执行sql
	 * @param sqlList
	 * @return
	 */
	public int[] execBatch(List<String>sqlList){
		String sqlArr[]=new String[sqlList.size()];
		int i=0;
		for(String sqlStr:sqlList){
			sqlArr[i++]=sqlStr;
		}
		return jdbcTemplate.batchUpdate(sqlArr);
	}
}