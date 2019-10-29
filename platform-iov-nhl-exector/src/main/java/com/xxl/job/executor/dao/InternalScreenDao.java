package com.xxl.job.executor.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.util.DateRegUtil;
import com.xxl.job.executor.util.JdbcUtil;

import net.tycmc.bulb.common.util.DateUtil;

@Component
public class InternalScreenDao {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	/**
	 * 查询近7天的日均每车工作小时
	 */
	public List<Map<String, Object>> getLastSevenAverageWorkHours(){
		String sql = "";
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//从昨天算，之前7天的日期
		String LastSevenday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(8), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到LastThirtyday 的年月
		String LastSevendayYM  = DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyyMM");
		//得到 昨天的年月
		String lastdayYM =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyyMM");
		if(LastSevendayYM.equals(lastdayYM)){
			sql+= "select DATE_FORMAT(daily_time, '%Y-%m-%d %T') as daily_time , case when sum(work_hours) is null then ROUND(0,2) else  ROUND(sum(work_hours),2) end  as sumworkhours,count(work_hours) as  sumcount from daily_data_"+lastdayYM + "where  DATE_FORMAT(daily_time, '%Y-%m-%d') >= "+LastSevendayYM+"and  DATE_FORMAT(daily_time, '%Y-%m-%d') <="+lastday+"  and warranty_status='1' group by daily_time ";
		}else{
			sql+= "select DATE_FORMAT(daily_time, '%Y-%m-%d %T') as daily_time, case when sum(work_hours) is null then ROUND(0,2) else  ROUND(sum(work_hours),2) end  as sumworkhours,count(work_hours) as  sumcount from(select work_hours,daily_time from daily_data_"+LastSevendayYM+" where   DATE_FORMAT(daily_time, '%Y-%m-%d') >= '"+LastSevenday+"'  and warranty_status='1'"
					+"UNION all "
					+"SELECT  work_hours,daily_time  from daily_data_"+lastdayYM+"  where  DATE_FORMAT(daily_time, '%Y-%m-%d') <='"+lastday+"' and warranty_status='1') v  group by  v.daily_time";
		}
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;

	}
	/**
	 * 查询日报近30天的数据（在保）
	 * @return
	 */
	public List<Map<String, Object>> GetLastThirtyDailyData(){
		String sql = "";
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//从昨天算，之前30天的日期
		String LastThirtyday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到LastThirtyday 的年月
		String LastThirtydayYM  = DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyyMM");
		//得到 昨天的年月
		String lastdayYM =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyyMM");
		if(LastThirtydayYM.equals(lastdayYM))
		{
			sql+="select * from daily_data_"+lastdayYM + "where   DATE_FORMAT(daily_time, '%Y-%m-%d') >= "+LastThirtyday+"and  DATE_FORMAT(daily_time, '%Y-%m-%d') <="+lastday+"and warranty_status='1'";
		}else
		{
			sql+="select * from daily_data_"+LastThirtydayYM+" where   DATE_FORMAT(daily_time, '%Y-%m-%d') >= '"+LastThirtyday+"' and warranty_status='1'"
					+" UNION all "
					+"SELECT * from daily_data_"+lastdayYM+" where    DATE_FORMAT(daily_time, '%Y-%m-%d') <="+lastday+" and warranty_status='1'";
		}
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;

	}
	/**
	 * 查询日报近30天的数据（除了报废车辆）
	 * @return
	 */
	public List<Map<String, Object>> GetLastThirtyDailyData2(){
		String sql = "";
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//从昨天算，之前30天的日期
		String LastThirtyday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到LastThirtyday 的年月
		String LastThirtydayYM  = DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyyMM");
		//得到 昨天的年月
		String lastdayYM =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyyMM");
		if(LastThirtydayYM.equals(lastdayYM))
		{
			sql+="select * from daily_data_"+lastdayYM + "where   DATE_FORMAT(daily_time, '%Y-%m-%d') >= "+LastThirtyday+"and  DATE_FORMAT(daily_time, '%Y-%m-%d') <="+lastday;
		}else
		{
			sql+="select * from daily_data_"+LastThirtydayYM+" where   DATE_FORMAT(daily_time, '%Y-%m-%d') >= '"+LastThirtyday+"'"
					+" UNION all "
					+"SELECT * from daily_data_"+lastdayYM+" where    DATE_FORMAT(daily_time, '%Y-%m-%d') <="+lastday;
		}
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;
		
	}
	/**
	 * 累计销售车辆
	 * 
	 * @return
	 */
	public Map<String, Object>getSallCarCount() {

		String sql = "SELECT count(VehicleNumber) as sallCarCount  from  v_vehicleinfo where OrgCode like '1001D1003%' ";
		return jdbcTemplate.queryForMap(sql);
	}
	/**
	 * 联网车辆数
	 * 
	 * @return
	 */
	public Map<String, Object>getConnectCarCount() {
		String sql = "select  count(1) as connectCarCount from V_VehicleInfo v  inner join Tmnl_Install_last t on v.PFVehicleId = SUBSTRING(t.TmnlIL_Vcl_ID, 5)  where 	retiredate>=now() and  OrgCode like '1001D1003%'";
		return jdbcTemplate.queryForMap(sql);
	}
	/**
	 * 昨日总拉运量
	 * @param 
	 */
	public Map<String, Object> getLastDayTransportSum(){
		String sql = "";
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到 昨天的年月
		String lastdayYM =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyyMM");
		sql +="select case when sum(transport) is null then ROUND(0,2) else ROUND(sum(transport),2) end  as transportsum from (select  d.daily_time,d.daily_iyuci * v.RatedLoad as transport FROM   daily_data_"+lastdayYM+" d "
				+ "inner join V_VehicleInfo v on  d.daily_vclid = v.VehicleNumber where DATE_FORMAT(d.daily_time, '%Y-%m-%d') ='"+lastday+"' and v.OrgCode like '1001D1003%') a ";
		Map<String, Object> queryForMap = jdbcTemplate.queryForMap(sql);
		return queryForMap;
	}
	/**
	 * 国内各省份车辆排名   前八名
	 * @param 
	 */
	public List<Map<String, Object>> getCarcountByProvince(){
			String sql ="SELECT  a.province, COUNT(a.VehicleNumber) as carCount "
					+ "from(SELECT o.province ,VehicleNumber FROM VehicleInfo v "
					+ "INNER JOIN organization_db o ON v.OrgId = o.OrganizationID "
					+ "WHERE o.OrganizationCode LIKE '1001D1003%' AND o.province IS NOT NULL "
					+ "AND o.province != '') a GROUP BY a.province order by COUNT(a.VehicleNumber) DESC limit 8";
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList ;

	}
	/**
	 * 海内外车辆占比(返回海外的) 注如果是7% 返回的就是7
	 * @param 
	 */
	public double getPercentage(){
		//国外车辆
		String sql ="SELECT  count(*) as abroadcarcount from vehicleinfo v  "
				+ "INNER JOIN   organization_db o on v.OrgId = o.OrganizationID  where country is not NULL and country!='' and  country !='中国' and o.OrganizationCode like '1001D1003%'";
		long abroadcarcount =  (long) jdbcTemplate.queryForMap(sql).get("abroadcarcount"); 
		//国内车辆
		sql ="select  count(*) as internalcarcount from vehicleinfo v "
				+ "INNER JOIN   organization_db o on v.OrgId = o.OrganizationID  where (country is NULL or country='' or  country ='中国') and o.OrganizationCode like '1001D1003%'";
		long internalcarcount =  (long) jdbcTemplate.queryForMap(sql).get("internalcarcount"); //国外车辆
		double sum =  abroadcarcount+internalcarcount;
		if(sum == 0){
			return 0;
		}
		return abroadcarcount/sum*100;
	}
	/**
	 * 国外车辆排名   前八名
	 * @param 
	 */
	public List<Map<String, Object>> getCarcountByAbroad(){
		String sql = "SELECT a.country ,a.carcount from (select country,count(v.VehicleNumber) as carcount from vehicleinfo v "
				+ "INNER JOIN   organization_db o on v.OrgId = o.OrganizationID  where country is not NULL and country!='' and  country !='中国'  and o.OrganizationCode like '1001D1003%'  GROUP BY o.country) a ORDER BY a.carcount desc limit 8";
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList ;

	}
	/**
	 * 驱动类型占比
	 * 
	 * @param 
	 */
	public List<Map<String, Object>> getClassPercentage(){
		//总的车型数量
		String sql ="select count(VehicleNumber) as sumClass from  V_VehicleInfo  v "
				+ " INNER JOIN  BasicData_VehicleClass b on v.VehicleClassId = b.VehicleClassID where v.OrgCode like '1001D1003%' ";
		long sumClass =  (long) jdbcTemplate.queryForMap(sql).get("sumClass"); 
		//各个车型的数量
		sql = "SELECT  min(b.VehicleClassName) as classname ,count(VehicleNumber) as carcount from  V_VehicleInfo  v  "
				+ "INNER JOIN  BasicData_VehicleClass b on v.VehicleClassId = b.VehicleClassID where v.OrgCode like '1001D1003%'  GROUP BY v.VehicleClassId";
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		for (Map<String, Object> map : queryForList) {
			if(sumClass == 0){
				map.put("percentage", 0);
			}else{
				long carcount = (long) map.get("carcount");
				map.put("percentage",  String.format("%.2f", (double)carcount/sumClass));
			}
		}
		return queryForList ;

	}
	/**
	 * 车型平均生产力
	 * 
	 * @param 
	 */
	public List<Map<String, Object>> getCarPercentageProductive(){
		String sql = "";
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//从昨天算，之前30天的日期
		String LastThirtyday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到LastThirtyday 的年月
		String LastThirtydayYM  = DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyyMM");
		//得到 昨天的年月
		String lastdayYM =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyyMM");
		if(LastThirtydayYM.equals(lastdayYM))
		{
			sql+="	SELECT d.VehicleTypeName,d.carproductive from (select min(c.VehicleTypeName) as VehicleTypeName ,"
					+ "case when count(b.VehicleNumber) = 0 then 0 else ROUND(SUM(b.cardayproductive)/count(b.VehicleNumber),2) end as carproductive "
					+ "from (SELECT  v.VehicleNumber ,v.VehicleTypeID, case when a.work_hours =0 then 0 else v.RatedLoad*a.daily_iyuci/a.work_hours end as cardayproductive "
					+ "from (select daily_vclid,daily_iyuci,work_hours from daily_data_"+lastdayYM+" where   DATE_FORMAT(daily_time, '%Y-%m-%d') >= '"+LastThirtyday+"' and   DATE_FORMAT(daily_time, '%Y-%m-%d') <='"+lastday+"' and warranty_status='1') a  "
					+ "inner join v_vehicleinfo v on a.daily_vclid  = v.VehicleNumber where v.OrgCode like '1001D1003%' ) b  inner join BasicData_VehicleType c on b.VehicleTypeID = c.VehicleTypeID "
					+ "group BY  b.VehicleTypeID) d order by d.carproductive desc  limit 6";
		}else
		{
			sql+="	SELECT d.VehicleTypeName,d.carproductive from (select  min(c.VehicleTypeName) as  VehicleTypeName ,"
					+ "case when count(b.VehicleNumber) = 0 then 0 else ROUND(SUM(b.cardayproductive)/count(b.VehicleNumber),2) end as carproductive "
					+ "from (SELECT  v.VehicleNumber ,v.VehicleTypeID, case when a.work_hours =0 then 0 else v.RatedLoad*a.daily_iyuci/a.work_hours end as cardayproductive "
					+ "from (select daily_vclid,daily_iyuci,work_hours from daily_data_"+LastThirtydayYM+" where   DATE_FORMAT(daily_time, '%Y-%m-%d') >= '"+LastThirtyday+"' and warranty_status='1'"
					+ "UNION all SELECT daily_vclid,daily_iyuci,work_hours from daily_data_"+lastdayYM+"  where    DATE_FORMAT(daily_time, '%Y-%m-%d') <='"+lastday+"' and warranty_status='1') a "
					+ " inner join v_vehicleinfo v on a.daily_vclid  = v.VehicleNumber where v.OrgCode like '1001D1003%') b  inner join BasicData_VehicleType c on b.VehicleTypeID = c.VehicleTypeID"
					+ " group BY  b.VehicleTypeID) d order by d.carproductive desc  limit 6";
		}
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;
	} 
	/**
	 * 车型平均吨消耗    前6名
	 * 
	 * @param 
	 */
	public List<Map<String, Object>> getTonConsumption(){
		String sql = "";
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//从昨天算，之前30天的日期
		String LastThirtyday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到LastThirtyday 的年月
		String LastThirtydayYM  = DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyyMM");
		//得到 昨天的年月
		String lastdayYM =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyyMM");
		if(LastThirtydayYM.equals(lastdayYM)){
			sql+="select  b.VehicleTypeName,a.tonconsumption  from (select min(v.VehicleTypeID) as VehicleTypeID, "
					+ "case when max(msl.MsgESSLT_iOilCons) = 0 then 0 ELSE ROUND( sum(d.daily_iyuci)*v.RatedLoad/max(msl.MsgESSLT_iOilCons),2) end as tonconsumption "  
					+ "from daily_data_"+lastdayYM+" d inner join v_vehicleinfo v on d.daily_vclid  = v.VehicleNumber "
					+ "inner join  Msg_EquipmentState_Statistics_LastThirty msl on  v.PFVehicleId = msl.MsgESSLT_VclID  where v.OrgCode like '1001D1003%' and "
					+ "  DATE_FORMAT(d.daily_time, '%Y-%m-%d') >= '"+LastThirtyday+"' and  DATE_FORMAT(d.daily_time, '%Y-%m-%d') <='"+lastday+"' and d.warranty_status='1' GROUP BY v.VehicleNumber)  a "
					+ " inner join BasicData_VehicleType b on  a.VehicleTypeID = b.VehicleTypeID order by a.tonconsumption desc limit 6";

		}else{
			sql+="select  b.VehicleTypeName,a.tonconsumption  from (select min(v.VehicleTypeID) as VehicleTypeID , "
					+ "case when max(msl.MsgESSLT_iOilCons) = 0 then 0 ELSE  ROUND(sum(d.daily_iyuci)*v.RatedLoad/max(msl.MsgESSLT_iOilCons),2) end as tonconsumption"
					+ " from (select  daily_vclid, daily_iyuci from  daily_data_"+LastThirtydayYM+" where  DATE_FORMAT(daily_time, '%Y-%m-%d') >= '"+LastThirtyday+"' and warranty_status='1' union all "
					+ "select daily_vclid,  daily_iyuci from   daily_data_"+lastdayYM+" where DATE_FORMAT(daily_time, '%Y-%m-%d') <='"+lastday+"' and warranty_status='1') d "
					+ "inner join v_vehicleinfo v on d.daily_vclid  = v.VehicleNumber inner join  Msg_EquipmentState_Statistics_LastThirty msl on  v.PFVehicleId = msl.MsgESSLT_VclID where v.OrgCode like '1001D1003%' GROUP BY v.VehicleNumber)  a  "
					+ "inner join BasicData_VehicleType b on  a.VehicleTypeID = b.VehicleTypeID order by a.tonconsumption desc limit 6";
		}
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;
	}
	/**
	 * 车型排名
	 * 
	 * @param 
	 */
	public List<Map<String, Object>> getCarTypeRanking(){
		String sql = "select a.VehicleTypeName , a.carcount  from (select  min(b.VehicleTypeName) as VehicleTypeName, "
				+ "count( v.VehicleNumber)  as  carcount from v_VehicleInfo  v"
				+ " inner join BasicData_VehicleType b on v.VehicleTypeId=b.VehicleTypeID  where v.OrgCode like '1001D1003%'  group by  b.VehicleTypeID  ) a  "
				+ "order by  a.carcount desc limit 10";
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;
	}
	/**
	 * 近30天拉运量   前10名
	 * @param 
	 */
	public List<Map<String, Object>> getLoadRanking(){
		String sql ="";
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//从昨天算，之前30天的日期
		String LastThirtyday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到LastThirtyday 的年月
		String LastThirtydayYM  = DateUtil.toString(DateUtil.toDate(DateUtil.subDay(31), "yyyy-MM-dd"), "yyyyMM");
		//得到 昨天的年月
		String lastdayYM =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyyMM");
		if(LastThirtydayYM.equals(lastdayYM)){
			sql+="select  a.daily_vclid , a.sumLoad from (select daily_vclid,case when sum(daily_iyuci) is null then round(0,2) else  round(sum(daily_iyuci)*RatedLoad/10000,2)  end as  sumLoad  "
					+ "from   daily_data_"+lastdayYM+" d   inner join v_vehicleinfo on daily_vclid =VehicleNumber"
					+ " where  DATE_FORMAT(d.daily_time, '%Y-%m-%d') >= '"+LastThirtyday+"' and  DATE_FORMAT(d.daily_time, '%Y-%m-%d') <='"+lastday+"' and OrgCode like '1001D1003%'"
					+ "group by  d.daily_vclid ) a order by a.sumLoad desc limit 10 ";

		}else{
			sql +="select a.daily_vclid , a.sumLoad from (select daily_vclid,case when sum(daily_iyuci) is null then round(0,2) else  round(sum(daily_iyuci)*RatedLoad/10000,2) end  as  sumLoad "
					+ "from   (select  daily_vclid, daily_iyuci,RatedLoad from  daily_data_"+LastThirtydayYM+" inner join v_vehicleinfo "
					+ " on daily_vclid =VehicleNumber where  DATE_FORMAT(daily_time, '%Y-%m-%d') >= '"+LastThirtyday+"'  and OrgCode like '1001D1003%' "
					+ "union all select daily_vclid,daily_iyuci,RatedLoad from   daily_data_"+lastdayYM+" inner join v_vehicleinfo "
					+ " on daily_vclid =VehicleNumber where DATE_FORMAT(daily_time, '%Y-%m-%d') <='"+lastday+"' and  OrgCode like '1001D1003%' ) d  "
					+ "group by  d.daily_vclid ) a order by a.sumLoad desc limit 10 ";
		}
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;
	}
	/**
	 * 车辆总运行小时  前10名
	 * @param 
	 */
	public List<Map<String, Object>> getSumWorkHoursRanking(){

		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到 昨天的年月
		String lastdayYM =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyyMM");
		String sql = "select  d.daily_vclid ,d.engine_total_hours from   daily_data_"+lastdayYM+"  d  "
				+ "where DATE_FORMAT(daily_time, '%Y-%m-%d') ='"+lastday+"' order by  d.engine_total_hours desc  limit 10";
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;

	}
	/**
	 *  昨天工作排名  前25名
	 * @param 
	 */
	public List<Map<String, Object>> getLastDayRanking(){
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到 昨天的年月
		String lastdayYM =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyyMM");
		String sql = "select   d.daily_vclid ,d.daily_iyuci ,d.work_hours  from   daily_data_"+lastdayYM+"  d  "
				+ "where DATE_FORMAT(daily_time, '%Y-%m-%d') ='"+lastday+"' order by  d.daily_iyuci desc  limit 25";
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;
	}
	/**
	 *  地区开工饱和度
	 * @param 
	 */
	public List<Map<String, Object>> getLocationWork(){
		String sql = "SELECT   o.province, case when COUNT(msl.MsgESSLS_VclID) =0 then 0 else round(sum(msl.MsgESSLS_iWork)/7/COUNT(msl.MsgESSLS_VclID)/8*100,2) end as locationwork"
				+ " from Msg_EquipmentState_Statistics_LastSeven msl inner join v_vehicleinfo v on v.PFVehicleId =msl.MsgESSLS_VclID "
				+ "inner join organization_db o on v.OrgCode = o.Organizationcode where o.country ='中国' or  o.country ='' or o.country is NULL and v.OrgCode like '1001D1003%' and v.RepairEndDate>NOW()  "
				+ "group by o.province ORDER BY locationwork LIMIT 10";
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;
	}
	/**
	 * 保存内部大屏数据到本地
	 */
	public int saveInternalScreenData (Map<String, String> mapParam){
		String sql = "INSERT INTO large_basic (vehicle_intact_rate,vehicle_attendance,mtbf,mttr,sales_vehicle,networked_vehicles,"
				+ "yesterday_totalpull_volum,average_work_hours,driver_type_proportion,average_vehicle_productivit,"
				+ "average_ton_vehicle,average_monthly_work,average_monthly_output,vehicl_ranking,haulage_capacity,total_hours,"
				+ "yesterday_work_ranking,contry_proportion,large_vehicle_province,regional_saturation,updatedatetime)"
				+ " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,Now())";
		return jdbcTemplate.update(sql, new Object[] {
				MapUtils.getString(mapParam, "vehicle_intact_rate"),
				MapUtils.getString(mapParam, "vehicle_attendance"),
				MapUtils.getString(mapParam, "mtbf"),
				MapUtils.getString(mapParam, "mttr"),
				MapUtils.getString(mapParam, "sales_vehicle"),
				MapUtils.getString(mapParam, "networked_vehicles"),
				MapUtils.getString(mapParam, "yesterday_totalpull_volum"),
				MapUtils.getString(mapParam, "average_work_hours"),
				MapUtils.getString(mapParam, "driver_type_proportion"),
				MapUtils.getString(mapParam, "average_vehicle_productivit"),
				MapUtils.getString(mapParam, "average_ton_vehicle"),
				MapUtils.getString(mapParam, "average_monthly_work"),
				MapUtils.getString(mapParam, "average_monthly_output"),
				MapUtils.getString(mapParam, "vehicl_ranking"),
				MapUtils.getString(mapParam, "haulage_capacity"),
				MapUtils.getString(mapParam, "total_hours"),
				MapUtils.getString(mapParam, "yesterday_work_ranking"),
				MapUtils.getString(mapParam, "contry_proportion"),
				MapUtils.getString(mapParam, "large_vehicle_province"),
				MapUtils.getString(mapParam, "regional_saturation"),
		});
	}
	/**
	 * 平均故障间隔时间 12个月的(没有12个月的，有几个算几个)
	 * @return
	 */
	public List<Double> getAverageFlatTime(){
		List<Double> resultList = new ArrayList<Double>();
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//12个月之前的日期
		String oneYearAgo =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(365), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到近12个的  年月，作为为查询月表的后缀 
		String[] monthSql = DateRegUtil.getMonthSql(oneYearAgo,lastday);
		for (String string : monthSql) {
			//判断月表是否存在
			String sql = "SELECT count(*) as count FROM information_schema.TABLES WHERE table_name ='daily_data_"+string+"'";
			long count = (long)jdbcTemplate.queryForMap(sql).get("count");
			if(count>0){ //存在
				sql ="select case when sum(work_hours) is null then ROUND(0,2) else ROUND(sum(work_hours),2) end as workhours ,count(daily_vclid) as sumcarcount "
						+ "from daily_data_"+string+" where work_hours is not null "  ;//当前月的总工作小时
				Map<String, Object> queryForMap = jdbcTemplate.queryForMap(sql);
				double  workhours  = (double)queryForMap.get("workhours");
				long  sumcarcount  = (long)queryForMap.get("sumcarcount");//当月所有车的记录数
				sql ="select count(fault_hours) as faultcount from daily_data_"+string+" where fault_result ='2' "  ;//当前月的总故障次数
				long  faultcount  = (long)jdbcTemplate.queryForMap(sql).get("faultcount");
				double oneMonthAverageFlatTime = 0;
				if(faultcount==0&&sumcarcount==0){
					oneMonthAverageFlatTime =0;
				}
				if(faultcount==0&&sumcarcount!=0){
					oneMonthAverageFlatTime = workhours/sumcarcount;
				}
				if(faultcount!=0&&sumcarcount!=0){
					oneMonthAverageFlatTime = workhours/faultcount/sumcarcount;
				}
				resultList.add(oneMonthAverageFlatTime);
			}
		}
		return resultList;
	}

	/**
	 * 平均故障间隔时间 12个月的(没有12个月的，有几个算几个)
	 * @return
	 */
	public List<Double> getAverageFlatRepairTime(){
		List<Double> resultList = new ArrayList<Double>();
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//12个月之前的日期
		String oneYearAgo =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(365), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到近12个的  年月，作为为查询月表的后缀 
		String[] monthSql = DateRegUtil.getMonthSql(oneYearAgo,lastday);
		for (String string : monthSql) {
			//判断月表是否存在
			String sql = "SELECT count(*) as count FROM information_schema.TABLES WHERE table_name ='daily_data_"+string+"'";
			long count = (long)jdbcTemplate.queryForMap(sql).get("count");
			if(count>0){ //存在
				sql ="select case when sum(fault_hours) is null then ROUND(0,2) else ROUND(sum(fault_hours),2) end as faulthours ,count(daily_vclid) as sumcarcount "
						+ "from daily_data_"+string+" where fault_hours is not null "  ;//当前月的总故障停机小时
				Map<String, Object> queryForMap = jdbcTemplate.queryForMap(sql);
				double  faulthours  = (double)queryForMap.get("faulthours");
				long  sumcarcount  = (long)queryForMap.get("sumcarcount");//当月所有车的记录数
				sql ="select count(fault_hours) as faultcount from daily_data_"+string+" where fault_result ='2'  "  ;//当前月的总故障次数
				long  faultcount  = (long)jdbcTemplate.queryForMap(sql).get("faultcount");
				double oneMonthAverageFlatRepairTime = 0;
				if(faultcount==0&&sumcarcount==0){
					oneMonthAverageFlatRepairTime =0;
				}
				if(faultcount==0&&sumcarcount!=0){
					oneMonthAverageFlatRepairTime = faulthours/sumcarcount;
				}
				if(faultcount!=0&&sumcarcount!=0){
					oneMonthAverageFlatRepairTime = faulthours/faultcount/sumcarcount;
				}
				resultList.add(oneMonthAverageFlatRepairTime);
			}
		}
		return resultList;
	}

	/**
	 * 车型月均小时(单个月的)
	 * @param 
	 */
	public List<Map<String, Object>> getCarTypeHourOil (String yearMonth){
		String sql = "SELECT count(*) as count FROM information_schema.TABLES WHERE table_name ='msg_equipmentstate_statistics_monthjh_"+yearMonth+"'";
		long count = (long)jdbcTemplate.queryForMap(sql).get("count");
		if(count>0){
			sql = "select  min(bv.VehicleTypeName) as VehicleTypeName , case when sum(MsgESM_iWork) is null then ROUND(0,2) else   ROUND(sum(MsgESM_iWork),2) end as monthaverage from "
					+"BasicData_VehicleType bv left join  v_vehicleinfo  v on   bv.VehicleTypeID = v.VehicleTypeID "
					+ "LEFT JOIN  msg_equipmentstate_statistics_monthjh_"+yearMonth+"  mesm  on v.PFVehicleId =  mesm.MsgESM_VclID"
					+" where v.OrgCode like '1001D1003%'  group by bv.VehicleTypeID  order by  v.VehicleTypeID";
			List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
			return queryForList;
		}else{
			return null;
		}

	}
	
	/**
	 * 车型平均产量(单个月的)
	 * @param 
	 */
	public List<Map<String, Object>> getCarTypeMonthOutput (String yearMonth){
		String sql = "SELECT count(*) as count FROM information_schema.TABLES WHERE table_name ='daily_data_"+yearMonth+"'";
		long count = (long)jdbcTemplate.queryForMap(sql).get("count");
		if(count>0){
			sql = " select min(a.VehicleTypeName)as cartype,ROUND(SUM(carLoad)/count(carLoad),2) as cartypeoutput "
					+ " FROM(select  bv.VehicleTypeName,bv.VehicleTypeID, case  when d.daily_iyuci*v.RatedLoad is null then 0 else d.daily_iyuci*v.RatedLoad  end as carLoad"
                    +" from   BasicData_VehicleType bv left join  v_vehicleinfo  v on   bv.VehicleTypeID = v.VehicleTypeID "
                     +"left join  daily_data_"+yearMonth+" d on d.daily_vclid = v.VehicleNumber  where v.OrgCode like '1001D1003%') a   group by a .VehicleTypeID order by  a.VehicleTypeID";
			List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
			return queryForList;
		}else{
			return null;
		}

	}
	/**
	 * 车辆位置
	 * @param 
	 */
	public  List<Map<String, Object>> carLocation (){
/*		String  sql = "SELECT VehicleNumber,CASE when tl.TmnlIL_Tmnl_ID  is NULL or tl.TmnlIL_Tmnl_ID =''"
				+ " then IFNULL(vcl.lo,'') else  IFNULL(MsgESL_Lo,'')  end as lo,CASE when tl.TmnlIL_Tmnl_ID  is NULL or "
				+ "tl.TmnlIL_Tmnl_ID ='' then IFNULL(vcl.la,'')  else  IFNULL(MsgESL_La,'')  end as la FROM vehicleinfo vcl"
				+ " LEFT JOIN organization_db o on vcl.OrgId = o.OrganizationID  "
				+ " LEFT JOIN Msg_EquipmentState_Last l ON vcl.PFVehicleId = l.MsgESL_VCLID "
				+ "LEFT JOIN tmnl_install_last tl ON vcl.PFVehicleId = SUBSTRING(tl.TmnlIL_Vcl_ID, 5) "
				+ "WHERE o.OrganizationCode LIKE '1001D1003%' and retiredate>=NOW() ";*/
		String sql ="SELECT VehicleNumber,CASE when l.MsgESL_VCLID  is NULL "
				+ "or l.MsgESL_VCLID ='' then IFNULL(vcl.lo,'') else  IFNULL(MsgESL_Lo,'') "
				+ " end as lo,CASE when l.MsgESL_VCLID  is NULL or  l.MsgESL_VCLID ='' then IFNULL(vcl.la,'')  "
				+ "else  IFNULL(MsgESL_La,'')  end as la FROM vehicleinfo vcl "
				+ "LEFT JOIN organization_db o on vcl.OrgId = o.OrganizationID  "
				+ "LEFT JOIN Msg_EquipmentState_Last l ON vcl.PFVehicleId = l.MsgESL_VCLID  "
				+ "WHERE o.OrganizationCode LIKE '1001D1003%' and retiredate>=NOW() ";
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sql);
		return queryForList;
	}
	/**
	 * 保存json格式的数据
	 * @param 
	 */
	public int saveJsonData(int type,  List<Map<String, Object>> data){
		int result =1;
		//插入sql
		String insertSql="";
		//删除sql
		String deleteSql="";
		//连接阿里云数据库使用
		JdbcUtil jdbcUtil = null;
		PreparedStatement pst = null;
		try {
			switch (type) {
			case 1: //日均每车工作小时
				deleteSql="TRUNCATE average_work_hours";
				insertSql ="insert into average_work_hours (dateTime,average_work_hours) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("datetime")+"");
					pst.setString(2, map.get("AverageWorkHhour")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
						map.get("datetime")+"",
						map.get("AverageWorkHhour")+""
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 2://驱动类型占比
				deleteSql="TRUNCATE driver_type_proportion";
				insertSql ="insert into driver_type_proportion (driver_type,proportion) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("classname")+"");
					pst.setString(2, map.get("percentage")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
						map.get("classname")+"",
						map.get("percentage")+""
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 3://车型平均生产力
				deleteSql="TRUNCATE average_vehicle_productivit";
				insertSql ="insert into average_vehicle_productivit (car_type,average_productivit) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("VehicleTypeName")+"");
					pst.setString(2, map.get("carproductive")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
						map.get("VehicleTypeName")+"",
						map.get("carproductive")+""
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 4://车型平均吨消耗
				deleteSql="TRUNCATE average_ton_vehicle";
				insertSql ="insert into average_ton_vehicle (car_type,average_ton) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("VehicleTypeName")+"");
					pst.setString(2, map.get("tonconsumption")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
							map.get("VehicleTypeName")+"",
							map.get("tonconsumption")+""
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 5://车型排名
				deleteSql="TRUNCATE vehicl_ranking";
				insertSql ="insert into vehicl_ranking (car_type,carcount) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("VehicleTypeName")+"");
					pst.setString(2, map.get("carcount")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
							map.get("VehicleTypeName")+"",
							map.get("carcount")+""
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 6://近30天拉运量top10
				deleteSql="TRUNCATE haulage_capacity";
				insertSql ="insert into haulage_capacity (vehicle_number,capacity) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("daily_vclid")+"");
					pst.setString(2, map.get("sumLoad")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
							map.get("daily_vclid")+"",
							map.get("sumLoad")+""
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 7://车辆总运行小时top10
				deleteSql="TRUNCATE total_hours";
				insertSql ="insert into total_hours (vehicle_number,total_hours) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("daily_vclid")+"");
					pst.setString(2, map.get("engine_total_hours")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
							map.get("daily_vclid")+"",
							map.get("engine_total_hours")+""
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 8://昨日工作排名(拉运趟次/工作小时)
				deleteSql="TRUNCATE yesterday_work_ranking";
				insertSql ="insert into yesterday_work_ranking (vehicle_number,layun_number,layun_hours) values (?,?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("daily_vclid")+"");
					pst.setString(2, map.get("daily_iyuci")+"");
					pst.setString(3, map.get("work_hours")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
							map.get("daily_vclid")+"",
							map.get("daily_iyuci")+"",
							map.get("work_hours")+""
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 9://国内各省车辆排名
				deleteSql="TRUNCATE large_vehicle_province";
				insertSql ="insert into large_vehicle_province (province,car_number) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("province")+"");
					pst.setString(2, map.get("carCount")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
							map.get("province")+"",
							map.get("carCount")+"",
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 10://地区开工饱和度
				deleteSql="TRUNCATE regional_saturation";
				insertSql ="insert into regional_saturation (province,saturation) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("province")+"");
					pst.setString(2, map.get("locationwork")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
							map.get("province")+"",
							map.get("locationwork")+"",
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			case 11://海外各国车辆排名
				deleteSql="TRUNCATE carcount_Abroad";
				insertSql ="insert into carcount_Abroad (country,carcount) values (?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				 pst = jdbcUtil.getPst();
					for (Map<String, Object> map : data) {
						pst.setString(1, map.get("country")+"");
						pst.setString(2, map.get("carcount")+"");
						pst.addBatch();
						jdbcTemplate.update(insertSql, new Object[]{
								map.get("country")+"",
								map.get("carcount")+"",
						});
					}
					pst.executeBatch();
					jdbcUtil.close();
				break;
			case 12://车辆位置
				deleteSql="TRUNCATE carLocation";
				insertSql ="insert into carLocation (vehicle_number,lo,la) values (?,?,?) ";
				jdbcTemplate.execute(deleteSql); //保存之前先删除原有的数据
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				jdbcUtil = new JdbcUtil(insertSql);
				pst = jdbcUtil.getPst();
				for (Map<String, Object> map : data) {
					pst.setString(1, map.get("VehicleNumber")+"");
					pst.setString(2, map.get("lo")+"");
					pst.setString(3, map.get("la")+"");
					pst.addBatch();
					jdbcTemplate.update(insertSql, new Object[]{
							map.get("VehicleNumber")+"",
							map.get("lo")+"",
							map.get("la")+""
					});
				}
				pst.executeBatch();
				jdbcUtil.close();
				break;
			}
		} catch (Exception e) {
			XxlJobLogger.log("存储类型："+type+"json数据出现异常"+e.getMessage());
			result = 0;
		}
		return result;
	}
	/**
	 * 保存车型月均产量和车型月均工作小时的结果
	 * @param 
	 */
	public int saveJsonDataCartype(int type, Map<String, Object> data){
		String deleteSql ="";
		String insertSql = "";
		JdbcUtil jdbcUtil = null;
		try {
			if(type==13){//车型月均工作小时
				deleteSql="TRUNCATE average_monthly_work";
				insertSql="INSERT INTO average_monthly_work (dateTime,car_type,average_work)  VALUES(?,?,?)";
				jdbcTemplate.execute(deleteSql);
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				
				jdbcUtil = new JdbcUtil(insertSql);
				PreparedStatement pst = jdbcUtil.getPst();
				Set<String> keySet = data.keySet();
				for (Iterator<String> iterator = keySet.iterator();iterator.hasNext();) {
					String datetime = iterator.next();
					List<Map<String, Object>> object = (List<Map<String, Object>>) data.get(datetime);
					for (Map<String, Object> map : object) {
						pst.setString(1, datetime);
						pst.setString(2, map.get("VehicleTypeName")+"");
						pst.setString(3, map.get("monthaverage")+"");
						pst.addBatch();
						jdbcTemplate.update(insertSql, new Object[]{
								datetime,
								map.get("VehicleTypeName")+"",
								map.get("monthaverage")+""
						});
					}
				}
				pst.executeBatch();
				jdbcUtil.close();
			}
			if(type==14){//车型月均产量
				deleteSql="TRUNCATE average_monthly_output";
				insertSql="INSERT INTO average_monthly_output (dateTime,car_type,output)  VALUES(?,?,?)";
				jdbcTemplate.execute(deleteSql);
				//aliyun 数据库删除
				jdbcUtil = new JdbcUtil(deleteSql);
				jdbcUtil.getPst().execute();
				jdbcUtil.close();
				
				jdbcUtil = new JdbcUtil(insertSql);
				PreparedStatement pst = jdbcUtil.getPst();
				Set<String> keySet = data.keySet();
				for (Iterator<String> iterator = keySet.iterator();iterator.hasNext();) {
					String datetime = iterator.next();
					List<Map<String, Object>> object = (List<Map<String, Object>>) data.get(datetime);
					for (Map<String, Object> map : object) {
						pst.setString(1, datetime);
						pst.setString(2, map.get("cartype")+"");
						pst.setString(3, map.get("cartypeoutput")+"");
						pst.addBatch();
						jdbcTemplate.update(insertSql, new Object[]{
								datetime,
								map.get("cartype")+"",
								map.get("cartypeoutput")+""
						});
					}
				}
				pst.executeBatch();
				jdbcUtil.close();
			}
		} catch (Exception e) {
			XxlJobLogger.log("存储类型："+type+"json数据出现异常"+e.getMessage());
			return 0;
		}
		return  1;
	}
/**
 * 保存海内外车辆占比
 * @param 
 */
	public  void saveInAndOut(double in ,double out ){
		try {
			String deleteSql="TRUNCATE carpercentage";
			String insertSql="INSERT INTO carpercentage (in_out,percentage)  VALUES(?,?)";
			jdbcTemplate.execute(deleteSql);
			JdbcUtil jdbcUtil = null;
			//aliyun 数据库删除
			jdbcUtil = new JdbcUtil(deleteSql);
			jdbcUtil.getPst().execute();
			jdbcUtil.close();
			jdbcUtil = new JdbcUtil(insertSql);
			PreparedStatement pst = jdbcUtil.getPst();
			pst.setString(1, "国内");
			pst.setString(2, String.format("%.2f", in));
			pst.addBatch();
			pst.setString(1, "海外");
			pst.setString(2, String.format("%.2f", out));
			pst.addBatch();
			pst.executeBatch();
			jdbcUtil.close();
			jdbcTemplate.update(insertSql, new Object[]{
					"国内",String.format("%.2f", in)
			});
			jdbcTemplate.update(insertSql, new Object[]{
					"海外",String.format("%.2f", out)
			});
		} catch (Exception e) {
			XxlJobLogger.log("海内外车辆占比出现异常："+e.getMessage());
		}
	}
	public static void main(String[] args) {
		InternalScreenDao dao = new InternalScreenDao(); 
		long a = 15;
		long b =8;
		System.out.println((double)b/a);
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(365), "yyyy-MM-dd"), "yyyy-MM-dd");
		String lastday1 =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		String[] monthSql = DateRegUtil.getMonthSql(lastday,lastday1 );
		for (String string : monthSql) {
			System.out.println(string);

		}

	}
}
