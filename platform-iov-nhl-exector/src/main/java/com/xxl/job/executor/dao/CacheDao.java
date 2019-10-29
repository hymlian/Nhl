package com.xxl.job.executor.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.xxl.job.core.log.XxlJobLogger;

@Component
public class CacheDao {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	/**
	 * 获取缓存表CLSET_Flag为0（未处理）的数据且对应时区的最大主键ID
	 * @return
	 */
	public Map<String,Object> getcachelsexectableMaxId(String condition){
		String sql=" SELECT MAX(CLSET_ID)maxid,MIN(CLSET_ID)minid FROM cachelsexectable INNER JOIN v_vehicleinfo ON CLSET_VclId=PFVehicleid INNER JOIN TimeZone ON TZ_ID=TimeZoneID WHERE CLSET_Flag=0 ";
		if(StringUtils.isNotBlank(condition)){
			sql+=" and "+condition;
		}
		Map<String,Object>map=new HashMap<String, Object>();
		try{
			map= jdbcTemplate.queryForMap(sql);
		}catch(Exception e){
			XxlJobLogger.log(e);
		}
		return map;
		
	}
	/**
	 * 获取缓存表CLSET_Flag为0（未处理）的数据
	 * @return
	 */
	public List<Map<String,Object>> getcachelsexectableList(String condition){
		String sql=" SELECT DISTINCT CLSET_VclId,CLSET_LastMsgTime FROM cachelsexectable INNER JOIN v_vehicleinfo ON CLSET_VclId=PFVehicleid INNER JOIN TimeZone ON TZ_ID=TimeZoneID WHERE CLSET_Flag=0 ";
		if(StringUtils.isNotBlank(condition)){
			sql+=" and "+condition;
		}
		return jdbcTemplate.queryForList(sql);
		 
		
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
