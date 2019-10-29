package com.xxl.job.executor.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import com.xxl.job.executor.dao.InternalScreenDao;
import com.xxl.job.executor.util.DateRegUtil;

import net.tycmc.bulb.common.util.DateUtil;


/**
 *  内部大屏
 * @author huyunmian
 *
 */
@Component
@Configuration
public class InternalScreenService {

	@Autowired
	private InternalScreenDao internalScreenDao;


	/**
	 * 近7天的日均每年工作小时
	 */
	public List<Map<String, Object>> getLastSevenAverageWorkHours(){
		List<Map<String, Object>> lastSevenAverageWorkHours = internalScreenDao.getLastSevenAverageWorkHours();
		return lastSevenAverageWorkHours;
	}
	/**
	 * 查询日报近30天的数据(只包含在保车辆)
	 */
	public List<Map<String, Object>> getLastThirtyDailyData (){
		List<Map<String, Object>> getLastThirtyDailyData = internalScreenDao.GetLastThirtyDailyData();
		return getLastThirtyDailyData;
	}
	/**
	 * 查询日报近30天的数据(除了报废车辆)
	 */
	public List<Map<String, Object>> getLastThirtyDailyData2 (){
		List<Map<String, Object>> getLastThirtyDailyData = internalScreenDao.GetLastThirtyDailyData2();
		return getLastThirtyDailyData;
	}

	/**
	 * 累计销售车辆
	 */
	public Map<String, Object> getSallCarCount() {
		Map<String, Object> carCount = internalScreenDao.getSallCarCount();
		return carCount;
	}
	/**
	 * 销售车辆数
	 */
	public Map<String, Object> getConnectCarCount() {
		Map<String, Object> carCount = internalScreenDao.getConnectCarCount();
		return carCount;
	}
	/**
	 * 昨日总拉运量
	 */
	public Map<String, Object> getLastDayTransportSum() {
		Map<String, Object> lastDayTransportSum = internalScreenDao.getLastDayTransportSum();
		return lastDayTransportSum;
	}
	/**
	 * 国内各省份车辆排名   前八名
	 * @param 
	 */
	public List<Map<String, Object>> getCarcountByProvince (){
		List<Map<String, Object>> carcountByProvince = internalScreenDao.getCarcountByProvince();
		return carcountByProvince;
	}
	/**
	 * 海内外车辆占比(返回海外的) 注如果是7% 返回的就是7
	 * @param 
	 */
	public double getPercentage(){
		return internalScreenDao.getPercentage();
	}
	/**
	 * 国外车辆排名   前八名
	 * @param 
	 */
	public List<Map<String, Object>> getCarcountByAbroad (){
		List<Map<String, Object>> carcountByAbroad = internalScreenDao.getCarcountByAbroad();
		return carcountByAbroad;
	}
	/**
	 * 驱动类型占比
	 * 
	 * @param args
	 */
	
	public List<Map<String, Object>> getClassPercentage(){
		List<Map<String, Object>> classPercentage = internalScreenDao.getClassPercentage();
		return  classPercentage ;
	}
	
	/**
	 * 车型平均生产力
	 * 
	 * @param 
	 */
	
	public  List<Map<String, Object>> getCarPercentageProductive(){
		List<Map<String, Object>> carPercentageProductive = internalScreenDao.getCarPercentageProductive();
		return carPercentageProductive;
	}
	
	/**
	 * 车型平均吨消耗    前7名
	 * 
	 * @param 
	 */
	public List<Map<String, Object>> getTonConsumption(){
		List<Map<String, Object>> tonConsumption = internalScreenDao.getTonConsumption();
		return tonConsumption;
	}
	/**
	 * 车型排名
	 * 
	 * @param 
	 */
	public List<Map<String, Object>> getCarTypeRanking(){
		List<Map<String, Object>> carTypeRanking = internalScreenDao.getCarTypeRanking();
		return carTypeRanking;
	}
	/**
	 * 近30天拉运量   前10名
	 * @param 
	 */
	public List<Map<String, Object>> getLoadRanking(){
		List<Map<String, Object>> loadRanking = internalScreenDao.getLoadRanking();
		return loadRanking;
	}
	/**
	 * 车辆总运行小时  前10名
	 * @param 
	 */
	public List<Map<String, Object>> getSumWorkHoursRanking(){
		List<Map<String, Object>> sumWorkHoursRanking = internalScreenDao.getSumWorkHoursRanking();
		return   sumWorkHoursRanking;
	}
	/**
	 *  昨天工作排名  前25名
	 * @param 
	 */
	public List<Map<String, Object>> getLastDayRanking(){
		List<Map<String, Object>> lastDayRanking = internalScreenDao.getLastDayRanking();
		return lastDayRanking;
	}
	/**
	 *  地区开工饱和度
	 * @param 
	 */
	public List<Map<String, Object>> getLocationWork(){
		List<Map<String,Object>> locationWork = internalScreenDao.getLocationWork();
		return locationWork;
	}
	/**
	 * 保存内部大屏数据到本地
	 */
	public int saveInternalScreenData (Map<String, String> mapParam){
        return internalScreenDao.saveInternalScreenData(mapParam);
		
	}
	/**
	 * 平均故障间隔时间 12个月的(没有12个月的，有几个算几个)
	 * @return
	 */
	public double getAverageFlatTime(){
		List<Double> averageFlatTime = internalScreenDao.getAverageFlatTime();
		if (null == averageFlatTime|| averageFlatTime.size() == 0) {
			return 0;
		}else{
			double sum =0;
			for (Double double1 : averageFlatTime) {
				sum +=double1;
			}
			return  sum/averageFlatTime.size();
		}
	}
	/**
	 * 平均故障修复时间 12个月的(没有12个月的，有几个算几个)
	 * @return
	 */
	public double getAverageFlatRepairTime(){
		List<Double> averageFlatRepairTime = internalScreenDao.getAverageFlatRepairTime();
		if (null == averageFlatRepairTime|| averageFlatRepairTime.size() == 0) {
			return 0;
		}else{
			double sum =0;
			for (Double double1 : averageFlatRepairTime) {
				sum +=double1;
			}
			return  sum/averageFlatRepairTime.size();
		}
	}
	
	/**
	 * 车型月均小时油耗(近12个月的) 有就统计
	 * @param 
	 */
	public Map<String, Object> getCarTypeHourOil(){
		Map<String, Object> resultMap = new HashMap<String, Object>();
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//12个月之前的日期
		String oneYearAgo =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(365), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到近12个的  年月，作为为查询月表的后缀 
		String[] monthSql = DateRegUtil.getMonthSql(oneYearAgo,lastday);
		for (int i =0;i<monthSql.length;i++){
			if(monthSql.length>12&&i==0){
				continue;
			}else{
				List<Map<String, Object>> carTypeHourOil = internalScreenDao.getCarTypeHourOil(monthSql[i]);
				if(null !=carTypeHourOil){//单个表存在
					resultMap.put(monthSql[i], carTypeHourOil);
				}
			}
			
		}
		
		return resultMap;
	}
	/**
	 * 车型平均产量(近12个月的) 有就统计
	 * @param 
	 */
	public Map<String, Object> getCarTypeMonthOutput(){
		Map<String, Object> resultMap = new HashMap<String, Object>();
		//昨天
		String lastday =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(1), "yyyy-MM-dd"), "yyyy-MM-dd");
		//12个月之前的日期
		String oneYearAgo =  DateUtil.toString(DateUtil.toDate(DateUtil.subDay(365), "yyyy-MM-dd"), "yyyy-MM-dd");
		//得到近12个的  年月，作为为查询月表的后缀 
		String[] monthSql = DateRegUtil.getMonthSql(oneYearAgo,lastday);
		for (int i =0;i<monthSql.length;i++){
			if(monthSql.length>12&&i==0){
				continue;
			}else{
				 List<Map<String, Object>> carTypeMonthOutput = internalScreenDao.getCarTypeMonthOutput(monthSql[i]);
					if(null !=carTypeMonthOutput){//单个表存在
						resultMap.put(monthSql[i], carTypeMonthOutput);
					}else{
						
					}
			}
			
		}
		return resultMap;
	}
	/**
	 * 车辆位置
	 * @param 
	 */
	public  List<Map<String, Object>> carLocation (){
		List<Map<String, Object>> carLocation = internalScreenDao.carLocation();
		return carLocation;
	}
	/**
	 * 保存json格式的数据
	 * @param 
	 */
	public int saveJsonData(int type,  List<Map<String, Object>> data){
		return internalScreenDao.saveJsonData(type, data);
	}
	/**
	 * 保存车型月均产量和车型月均工作小时的结果
	 * @param 
	 */
	public int saveJsonDataCartype(int type, Map<String, Object> data){
		return internalScreenDao.saveJsonDataCartype(type, data);
	}
	/**
	 * 保存海内外车辆数占比
	 */
	public void saveInAndOut(double in ,double  out){
		internalScreenDao.saveInAndOut(in, out);
	}
}
