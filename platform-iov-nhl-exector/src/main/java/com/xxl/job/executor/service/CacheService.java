package com.xxl.job.executor.service;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.xxl.job.executor.dao.CacheDao;

@Component
public class CacheService {
	@Autowired
	public CacheDao cacheDao;
	/**
	 * 获取缓存表CLSET_Flag为0（未处理）的数据
	 * @return
	 */
	public List<Map<String,Object>> getcachelsexectableList(String condition){
		return cacheDao.getcachelsexectableList(condition);
	}
	/**
	 * 获取缓存表CLSET_Flag为0（未处理）的数据
	 * @return
	 */
	public Map<String,Object> getcachelsexectableMaxId(String condition){
		return cacheDao.getcachelsexectableMaxId(condition);
	}
	/**
	 * 统计月表sql集合
	 */
	public List<String> execMaintCacheService(List<Map<String, Object>> list){
		List<String> sqlList=new ArrayList<String>();
		for (Map<String, Object> map : list) {
			
			int MsgESS_VclID = MapUtils.getIntValue(map, "MsgESS_VclID");
			String MsgESS_kmsdeviceId = MapUtils.getString(map, "MsgESS_kmsdeviceId");
			String MsgESS_StatisticsTime = MapUtils.getString(map, "MsgESS_StatisticsTime");
			
			
			
			String day = MapUtils.getString(map, "day");
			String MsgEss_uniqueness = MapUtils.getString(map, "MsgEss_uniqueness");
			Double MsgESS_iWork = MapUtils.getDouble(map, "MsgESS_iWork");
			Double MsgESS_tWork = MapUtils.getDouble(map, "MsgESS_tWork");
			Double MsgESS_iOilCons = MapUtils.getDouble(map, "MsgESS_iOilCons");
			Double MsgESS_tOilCons = MapUtils.getDouble(map, "MsgESS_tOilCons");
			Double MsgESS_iIdleWorkTime_PGN = MapUtils.getDouble(map, "MsgESS_iIdleWorkTime_PGN");
			Double MsgESS_tIdleWorkTime_PGN = MapUtils.getDouble(map, "MsgESS_tIdleWorkTime_PGN");
			Double MsgESS_iIdleWorkTime_UDS = MapUtils.getDouble(map, "MsgESS_iIdleWorkTime_UDS");
			Double MsgESS_tIdleWorkTime_UDS = MapUtils.getDouble(map, "MsgESS_tIdleWorkTime_UDS");
			
			String sql="";
			sql="insert into msg_equipmentstate_statistics_"+day+"(MsgEss_uniqueness,MsgESS_VclID,MsgESS_kmsdeviceId,MsgESS_StatisticsTime,MsgESS_iWork,MsgESS_tWork,MsgESS_iOilCons,MsgESS_tOilCons,MsgESS_iIdleWorkTime_PGN,MsgESS_tIdleWorkTime_PGN,MsgESS_iIdleWorkTime_UDS,MsgESS_tIdleWorkTime_UDS)"
					+ "values('"+MsgEss_uniqueness+"','"+MsgESS_VclID+"','"+MsgESS_kmsdeviceId+"','"+MsgESS_StatisticsTime+"','"+MsgESS_iWork+"','"+MsgESS_tWork+"','"+MsgESS_iOilCons+"','"+MsgESS_tOilCons+"','"+MsgESS_iIdleWorkTime_PGN+"','"+MsgESS_tIdleWorkTime_PGN+"','"+MsgESS_iIdleWorkTime_UDS+"','"+MsgESS_tIdleWorkTime_UDS+"') "
					+ "on duplicate key update MsgESS_VclID='"+MsgESS_VclID+"',MsgESS_kmsdeviceId='"+MsgESS_kmsdeviceId+"',MsgESS_StatisticsTime='"+MsgESS_StatisticsTime+"'"
					+ ",MsgESS_iWork='"+MsgESS_iWork+"',MsgESS_tWork='"+MsgESS_tWork+"',MsgESS_iOilCons='"+MsgESS_iOilCons+"',MsgESS_tOilCons='"+MsgESS_tOilCons+"'"
					+ ",MsgESS_iIdleWorkTime_PGN='"+MsgESS_iIdleWorkTime_PGN+"',MsgESS_tIdleWorkTime_PGN='"+MsgESS_tIdleWorkTime_PGN+"',MsgESS_iIdleWorkTime_UDS='"+MsgESS_iIdleWorkTime_UDS+"',MsgESS_tIdleWorkTime_UDS='"+MsgESS_tIdleWorkTime_UDS+"'";
			sqlList.add(sql);
			
		}
		return sqlList;
	}
	/**
	 * 统计月表sql集合
	 */
	public boolean execUpdate(Map<String, Object>map,String maxid,String minid){
		List<String> sqlList=new ArrayList<String>();
		for(String vcl:map.keySet()){
			sqlList.add("update cachelsexectable set CLSET_Flag=1 where CLSET_VclId='"+vcl+"' and CLSET_ID<="+maxid+" and CLSET_ID>="+minid);
		}
		boolean flag=execBatch(sqlList);
		return flag;
	}
	/**
	 * 统计自然月表sql集合
	 */
	public List<String> execMaintMonthjhService(List<Map<String, Object>> list){
		List<String> sqlList=new ArrayList<String>();
		for (Map<String, Object> map : list) {
			
			int MsgESM_VclID = MapUtils.getIntValue(map, "MsgESM_VclID");
			
			String day = MapUtils.getString(map, "day");
			Double MsgESM_iWork = MapUtils.getDouble(map, "MsgESM_iWork");
			Double MsgESM_tWork = MapUtils.getDouble(map, "MsgESM_tWork");
			Double MsgESM_iOilCons = MapUtils.getDouble(map, "MsgESM_iOilCons");
			Double MsgESM_tOilCons = MapUtils.getDouble(map, "MsgESM_tOilCons");
			Double MsgESM_iIdleWorkTime_PGN = MapUtils.getDouble(map, "MsgESM_iIdleWorkTime_PGN");
			Double MsgESM_tIdleWorkTime_PGN = MapUtils.getDouble(map, "MsgESM_tIdleWorkTime_PGN");
			Double MsgESM_iIdleWorkTime_UDS = MapUtils.getDouble(map, "MsgESM_iIdleWorkTime_UDS");
			Double MsgESM_tIdleWorkTime_UDS = MapUtils.getDouble(map, "MsgESM_tIdleWorkTime_UDS");
			
			String sql="";
			sql="insert into msg_equipmentstate_statistics_monthjh_"+day+"(MsgESM_VclID,MsgESM_iWork,MsgESM_iOilCons,MsgESM_iIdleWorkTime_PGN,MsgESM_iIdleWorkTime_UDS)"
					+ "values('"+MsgESM_VclID+"','"+MsgESM_iWork+"','"+MsgESM_iOilCons+"','"+MsgESM_iIdleWorkTime_PGN+"','"+MsgESM_iIdleWorkTime_UDS+"') "
					+ "on duplicate key update MsgESM_iWork='"+MsgESM_iWork+"',MsgESM_iOilCons='"+MsgESM_iOilCons+"',MsgESM_iIdleWorkTime_PGN='"+MsgESM_iIdleWorkTime_PGN+"',MsgESM_iIdleWorkTime_UDS='"+MsgESM_iIdleWorkTime_UDS+"'";
			sqlList.add(sql);
			
		}
		return sqlList;
	}
	public List<String> execMaintCachelsService(List<Map<String, Object>> list){
		List<String> sqlList=new ArrayList<String>();
		for (Map<String, Object> map : list) {
			String CLSET_VclId = MapUtils.getString(map, "CLSET_VclId");
			String CLSET_LastMsgTime = MapUtils.getString(map, "CLSET_LastMsgTime");
			
			String sql="";
			sql="update cachelsexectable set CLSET_Flag = 1 where CLSET_VclId ='"+CLSET_VclId+"' and CLSET_LastMsgTime='"+CLSET_LastMsgTime+"'";
			sqlList.add(sql);
			
		}
		return sqlList;
	}
	
	
	/**
	 * 批量执行sql
	 * @param sqlList
	 * @return
	 */
	public boolean execBatch(List<String>sqlList){
		int[] result= cacheDao.execBatch(sqlList);
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
	//传入时间所在月的第一天
	public static Date getFirstDayDateOfMonth(final Date date) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        final int last = cal.getActualMinimum(Calendar.DAY_OF_MONTH);
        cal.set(Calendar.DAY_OF_MONTH, last);
        return cal.getTime();
    }
	//传入时间所在月的最后一天
	public static Date getLastDayOfMonth(final Date date) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        final int last = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        cal.set(Calendar.DAY_OF_MONTH, last);
        return cal.getTime();
    }
}
