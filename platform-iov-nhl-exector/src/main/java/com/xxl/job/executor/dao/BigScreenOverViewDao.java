package com.xxl.job.executor.dao;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import net.tycmc.bulb.common.util.DateUtil;

@Component
public class BigScreenOverViewDao {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Value("${TimeSecond}")
	private String TimeSecond;
	
	public boolean dealWithBigScreenData(){
		//获取昨天时间
		String date = DateUtil.addDay(-1);
		String tabName = date.replace("-", "").substring(0, 6);
		String nowDate = DateUtil.addDay(0)+" 00:00:00";
		//i.	总设备数
		String sql1 = "SELECT COUNT(1) COUNT FROM v_vehicleinstallinfo WHERE Tmnl_ID IS NOT NULL";
		int countAll = 0;
		try {
			countAll = jdbcTemplate.queryForObject(sql1, new Object[]{},Integer.class);
		} catch (DataAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//ii.	平均油耗
		String sql2 = "SELECT ROUND(SUM(MsgESS_iOilCons)/SUM(MsgESS_iWork),0) AS oilHour FROM Msg_EquipmentState_Statistics_"+tabName+" "+
					  "INNER JOIN v_vehicleinstallinfo ON MsgESS_VclID=PFVehicleId WHERE MsgESS_iWork>0 AND Tmnl_ID IS NOT NULL AND MsgESS_StatisticsTime='"+date+"'";
		//String sql2 = "SELECT ROUND(SUM(MsgESS_iOilCons)/SUM(MsgESS_iWork),0) AS oilHour FROM Msg_EquipmentState_Statistics_"+tabName+" WHERE MsgESS_iWork>0 AND MsgESS_StatisticsTime='"+date+"' ";
		double avgOil = 0.00;
		try {
			avgOil = jdbcTemplate.queryForObject(sql2, new Object[]{},Double.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//iii.	全球设备统计
		List<Map<String,Object>> posList = new ArrayList<Map<String,Object>>();
		String sql3 = "SELECT PFVehicleid,VehicleNumber,UserVehicleNumber,MsgESL_Lo,MsgESL_La,"
				+ "CASE WHEN MsgESL_EgnState = 1 THEN CASE WHEN MsgESL_EgnState_MsgTime=NULL THEN 0 WHEN TIMESTAMPDIFF(SECOND,MsgESL_EgnState_MsgTime,NOW())>"+60+"*"+TimeSecond+" THEN 0  ELSE 1 END ELSE 0 END as MsgESL_EgnState "
				+ " FROM Msg_EquipmentState_Last mel INNER JOIN v_vehicleinstallinfo vf ON vf.PFVehicleId=mel.MsgESL_VCLID WHERE Tmnl_ID IS NOT NULL";
		posList = jdbcTemplate.queryForList(sql3);
		//v.	车辆数量统计：统计每个车型的占比
		String sql4 = "SELECT VehicleTypeID,a.cnt,CONCAT(ROUND(a.cnt/b.sum *100,2),'','%') AS percent "+
						"FROM "+
						"( "+
						"SELECT bvt.VehicleTypeID, "+
						"       COUNT(VehicleTypeName) AS cnt "+ 
						"FROM "+
						"       v_vehicleinstallinfo vvf INNER JOIN basicdata_vehicletype bvt ON vvf.VehicleTypeId=bvt.VehicleTypeID WHERE Tmnl_ID IS NOT NULL "+
						"       AND NOW()>EffectiveDate AND NOW()<ExpirationDate "+
						"GROUP BY "+
						"       bvt.VehicleTypeID "+ 
						"ORDER BY "+
						"       cnt DESC "+ 
						")AS a, "+
						"( "+
						"SELECT COUNT(*) AS SUM "+ 
						"FROM "+
						"       v_vehicleinstallinfo vvf INNER JOIN basicdata_vehicletype bvt ON vvf.VehicleTypeId=bvt.VehicleTypeID WHERE Tmnl_ID IS NOT NULL "+
						"       AND NOW()>EffectiveDate AND NOW()<ExpirationDate "+
						") AS b;";
		List<Map<String,Object>> typePerList = new ArrayList<Map<String,Object>>();
		typePerList = jdbcTemplate.queryForList(sql4);
		
		//vi.	故障报警统计
		int Engfault = 0;
		int EleFault = 0;
		String sql5 = "SELECT MsgFH_FaultType,COUNT(MsgFH_FaultType) AS COUNT FROM Msg_Flt_Happening INNER JOIN Sys_FltISM "+
						"ON (MsgFH_FaultFMI=SysISM_FMI AND MsgFH_FaultSPN=SysISM_SPN AND sysism_type=1) OR (MsgFH_Code=SysISM_Code AND sysism_type=2) "+
						"INNER JOIN v_vehicleinstallinfo ON MsgFH_Vcl_ID=PFVehicleId "+
						"WHERE MsgFH_MsgTime<'"+nowDate+"' AND Tmnl_ID IS NOT NULL GROUP BY MsgFH_FaultType";
		//String sql5 = "SELECT MsgFH_FaultType,COUNT(MsgFH_FaultType) AS COUNT FROM Msg_Flt_Happening WHERE MsgFH_MsgTime<'"+nowDate+"' GROUP BY MsgFH_FaultType";
		List<Map<String,Object>> faultList = new ArrayList<Map<String,Object>>();
		faultList = jdbcTemplate.queryForList(sql5);
		for(Map<String,Object> mmap : faultList){
			if("1".equals(MapUtils.getString(mmap, "MsgFH_FaultType"))){
				Engfault = MapUtils.getIntValue(mmap, "count");
			}
			if("2".equals(MapUtils.getString(mmap, "MsgFH_FaultType"))){
				EleFault = MapUtils.getIntValue(mmap, "count");
			}
		}
		
		//vii.	开工饱和度(近七天)
		List<Map<String,Object>> kgList = new ArrayList<Map<String,Object>>();
		String sql6 = "";
		for(int i=7;i>0;i--){
			String dateTime = DateUtil.addDay(0-i);
			String tabNameSub = DateUtil.addDay(0-i).replace("-", "").substring(0, 6);
			if(i==1){
				sql6 += "SELECT '"+dateTime+"' AS DATETIME, SUM(MsgESS_iWork) AS ihour,COUNT(1) AS cnt FROM Msg_EquipmentState_Statistics_"+tabNameSub+" "+
						"INNER JOIN v_vehicleinstallinfo ON MsgESS_VclID=PFVehicleId WHERE Tmnl_ID IS NOT NULL AND MsgESS_iWork>0 AND MsgESS_StatisticsTime='"+dateTime+"'";
			}else{
				sql6 += "SELECT '"+dateTime+"' AS DATETIME, SUM(MsgESS_iWork) AS ihour,COUNT(1) AS cnt FROM Msg_EquipmentState_Statistics_"+tabNameSub+" "+
						"INNER JOIN v_vehicleinstallinfo ON MsgESS_VclID=PFVehicleId WHERE Tmnl_ID IS NOT NULL AND MsgESS_iWork>0 AND MsgESS_StatisticsTime='"+dateTime+"' union all ";
			}
		}
		kgList = jdbcTemplate.queryForList(sql6);
		//处理数据
		DecimalFormat df = new DecimalFormat("######0.0000");
		for( Map<String,Object> mmap : kgList){
			if(StringUtils.isNotBlank(MapUtils.getString(mmap, "ihour")) && MapUtils.getInteger(mmap, "cnt")>0){
				double percent = MapUtils.getDoubleValue(mmap, "ihour")/(24*MapUtils.getInteger(mmap, "cnt"));
				mmap.put("percent", df.format(percent));
			}else{
				mmap.put("percent", "0.00");
			}
			
		}
		
		//将以上查询数据存入数据库（入库以前将旧数据清空）
		//将总设备数，昨日平均油耗，故障信息入库
		String[] update1 = new String[2];
		update1[0] = "DELETE FROM DataVShowAllVclStatic; ";		
		update1[1] = "INSERT INTO DataVShowAllVclStatic (DVSAVS_VclCount,DVSAVS_OilHour,DVSAVS_AllVclFlt,DVSAVS_EsnFlt,DVSAVS_UpdateTime) VALUES ("+countAll+","+avgOil+","+EleFault+","+Engfault+",NOW());";
		jdbcTemplate.batchUpdate(update1);
		
		String[] update2 = new String[typePerList.size()+1];
		update2[0] = "DELETE FROM DataVShowVclTypeStatic; ";
		int i1=1;
		for(Map<String,Object> mmap : typePerList){
			update2[i1] = "INSERT INTO DataVShowVclTypeStatic(DVVTS_TypeID,DVVTS_Count,DVVTS_UpdateTime) VALUES ('"+MapUtils.getString(mmap, "VehicleTypeID")+"',"+MapUtils.getIntValue(mmap, "cnt")+",NOW());";
			i1++;
		}
		jdbcTemplate.batchUpdate(update2);
		
		String[] update3 = new String[posList.size()+1];
		update3[0] = "DELETE FROM DataVShowVclPstnStatic; ";
		int i2=1;
		for(Map<String,Object> mmap : posList){
			update3[i2] = "INSERT INTO DataVShowVclPstnStatic (DVVPS_VehicleID,DVVPS_Lo,DVVPS_La,DVVPS_UpdateTime,DVVPS_EgnState) "
					+ "VALUES ('"+MapUtils.getString(mmap, "PFVehicleid")+"',"+MapUtils.getDoubleValue(mmap, "MsgESL_Lo")+","+MapUtils.getDoubleValue(mmap, "MsgESL_La")+",NOW(),"+MapUtils.getString(mmap, "MsgESL_EgnState")+");";
			i2++;
		}
		jdbcTemplate.batchUpdate(update3);
		
		String[] update4 = new String[kgList.size()+1];
		update4[0] = "DELETE FROM DataVShowVclWorkBHLevel; ";
		int i3 = 1;
		for(Map<String,Object> mmap : kgList){
			update4[i3] = "INSERT INTO DataVShowVclWorkBHLevel(DVVEBL_Date,DVVPS_WorkBHLevel,DVVPS_UpdateTime) "
					+ "VALUES('"+MapUtils.getString(mmap, "datetime")+"',"+MapUtils.getString(mmap, "percent")+",NOW());";
			i3++;
		}
		jdbcTemplate.batchUpdate(update4);
		
		return true;
	}
	
	public static void main(String[] args) {
		List<String> posList = new ArrayList<String>();
		posList.add("1");
		posList.add("2");
		posList.add("3");
		String[] update3 = new String[posList.size()+1];
		update3[0] = "DELETE FROM DataVShowVclPstnStatic; ";
		int i2=1;
		for(String mmap : posList){
			update3[i2] = mmap;
			i2++;
		}
		System.out.println();
	}
}
