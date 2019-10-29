package com.xxl.job.executor.dao;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;


@Component
public class VclStopRecordDao {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	/**查询对应设备
	 * @param condition
	 * @return
	 */
	public List<Map<String,Object>> getVehicleInfo(){
		String sql=	  "SELECT  PFVehicleId FROM vehicleinfo INNER JOIN tmnl_install_last tl ON  PFVehicleId =SUBSTRING(tl.TmnlIL_Vcl_ID,5)  WHERE PFVehicleId IS NOT NULL AND  LENGTH(PFVehicleId)<8  ";
		return jdbcTemplate.queryForList(sql);
	}
	
	public int batchInsert(List<String> sqlList) {
		
		int i=0;
		if(null!=sqlList&&sqlList.size()>0) {
			String sqlArr[]=new String[sqlList.size()];
			for(String sqlStr:sqlList){
				sqlArr[i++]=sqlStr;
			}
			try {
				jdbcTemplate.batchUpdate(sqlArr);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
				return -1;
			}
			return 1;
		}
		return i;
	}
	 
}
