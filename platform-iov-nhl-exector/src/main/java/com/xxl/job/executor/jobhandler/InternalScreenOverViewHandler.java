package com.xxl.job.executor.jobhandler;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.service.InternalScreenService;
import com.xxl.job.executor.util.JdbcUtil;


@JobHandler(value="InternalScreenOverViewHandler")
@Component
public class InternalScreenOverViewHandler extends IJobHandler{

	@Autowired
	private InternalScreenService internalScreenService;
	

	@Override
	public ReturnT<String> execute(String arg0) throws Exception {
		// TODO Auto-generated method stub
		XxlJobLogger.log("内部大屏统计开始：");
		//近7天的日均每车工作小时
		List<Map<String, Object>> lastlastSevenAverageWorkHours = new ArrayList<>();
		//获取近30天的日报数据，只包含在保车辆
		List<Map<String, Object>> getLastThirtyDailyData = internalScreenService.getLastThirtyDailyData();
		//获取近30天的日报数据，除了报废车辆
		List<Map<String, Object>> getLastThirtyDailyData2 = internalScreenService.getLastThirtyDailyData2();
		int size = getLastThirtyDailyData.size();
		int size2 = getLastThirtyDailyData2.size();
		//总的完好率
		double sumAvailability =0;
		//总的出勤率
		double sumAttendance = 0;
		for (Map<String, Object> map : getLastThirtyDailyData2) {
			double WT =  null == map.get("seheduling_hours") ?0:(double) map.get("seheduling_hours"); //日计划工作小时
			double  OT = null == map.get("work_hours") ?0: (double) map.get("work_hours");  //工作小时
			if(WT==0){
				sumAttendance += 0 ;
			}else{
				sumAttendance += OT/WT;
			}
		}
		for (Map<String, Object> map : getLastThirtyDailyData) {
			double WT =  null == map.get("seheduling_hours") ?0:(double) map.get("seheduling_hours"); //日计划工作小时
			double  UM = null == map.get("fault_hours") ?0:(double) map.get("fault_hours"); //故障停机时间
			if(WT==0){
				sumAvailability+= 1;
			}else{
				sumAvailability+=(WT-UM)/WT;
			}
		}
		//平均完好率
		sumAvailability = sumAvailability/size;
		//平均出勤率
		sumAttendance = sumAttendance/size2;
		//平均故障间隔时间 12个月的
		double averageFlatTime = internalScreenService.getAverageFlatTime();
		//平均故障修复时间 12个月的
		double averageFlatRepairTime = internalScreenService.getAverageFlatRepairTime();
		// 地区开工饱和度
		List<Map<String, Object>> locationWork = internalScreenService.getLocationWork();
		//获取近7天的日报数据，只包含在保车辆
		List<Map<String, Object>> lastSevenAverageWorkHours = internalScreenService.getLastSevenAverageWorkHours();
		//日均每车工作小时
		for (Map<String, Object> map : lastSevenAverageWorkHours) {
			HashMap<String, Object> flag = new  HashMap<>();
			flag.put("datetime", map.get("daily_time"));
			//某一天的所有车的工作小时
			double sumworkhours =   null == map.get("sumworkhours") ?0:(double) map.get("sumworkhours");
			//某一天的所有车总数
			double sumcount =   null ==  map.get("sumcount") ?0:(long) map.get("sumcount");
			//这一天的平均工作小时
			double  AverageWorkHhour =  0;
			if(sumcount == 0){
				AverageWorkHhour = 0;
			}else{
				AverageWorkHhour = sumworkhours/sumcount;
			}
			flag.put("AverageWorkHhour", AverageWorkHhour);
			lastlastSevenAverageWorkHours.add(flag);
		}
		//累计销售车辆
		long sallCarCount = (long) internalScreenService.getSallCarCount().get("sallCarCount");
		//联网车辆
		long  connectCarCount = (long)internalScreenService.getConnectCarCount().get("connectCarCount");
		//昨日总拉运量
		double lastDayTransportSum  = internalScreenService.getLastDayTransportSum().get("transportsum") ==null?0:(double) internalScreenService.getLastDayTransportSum().get("transportsum")/10000;
		// 国内各省份车辆排名   前八名
		List<Map<String, Object>> carcountByProvince = internalScreenService.getCarcountByProvince();
		//海外车辆占比
		double percentage = internalScreenService.getPercentage();
		//国内车辆占比
		double insidePercentage = 100 - percentage;
		//海外各国车辆数排名
		List<Map<String, Object>> carcountByAbroad = internalScreenService.getCarcountByAbroad();
		//驱动占比
		List<Map<String, Object>> classPercentage = internalScreenService.getClassPercentage();
		//车型平均生产力
		List<Map<String, Object>> carPercentageProductive = internalScreenService.getCarPercentageProductive();
		//车型平均吨消耗
		List<Map<String, Object>> tonConsumption = internalScreenService.getTonConsumption();
		//车型月均工作小时  12个月的
		Map<String, Object> carTypeHourOil = internalScreenService.getCarTypeHourOil();
		//车型月均产 量     12个月的
		Map<String, Object> carTypeMonthOutput = internalScreenService.getCarTypeMonthOutput();
		//车型排名
		List<Map<String, Object>> carTypeRanking = internalScreenService.getCarTypeRanking();
		//近30天拉运量   前10名
		List<Map<String, Object>> loadRanking = internalScreenService.getLoadRanking();
		//车辆总运行小时  前十名
		List<Map<String, Object>> sumWorkHoursRanking = internalScreenService.getSumWorkHoursRanking();
		// 昨天工作排名  前25名
		List<Map<String, Object>> lastDayRanking = internalScreenService.getLastDayRanking();
		//车辆位置
		List<Map<String, Object>> carLocation = internalScreenService.carLocation();
		//保存内部大屏的数据
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String>  param  =  new HashMap<String, String>();
		param.put("vehicle_intact_rate",sumAvailability+"");
		param.put("vehicle_attendance",sumAttendance+"");
		param.put("mtbf",averageFlatTime+"");
		param.put("mttr",averageFlatRepairTime+"");
		param.put("sales_vehicle",sallCarCount+"");
		param.put("networked_vehicles",connectCarCount+"");//connectCarCount
		param.put("yesterday_totalpull_volum",lastDayTransportSum+"");
		//保存海内外车辆占比
		internalScreenService.saveInAndOut(insidePercentage, percentage);
		//保存车型月均工作小时
		internalScreenService.saveJsonData(1, lastlastSevenAverageWorkHours);
		//保存驱动类型占比
		internalScreenService.saveJsonData(2, classPercentage);
		//车型平均生产力
		internalScreenService.saveJsonData(3, carPercentageProductive);
		//车型平均吨消耗
		internalScreenService.saveJsonData(4, tonConsumption);
		//车型排名
		internalScreenService.saveJsonData(5, carTypeRanking);
		//近30天拉运量top10
		internalScreenService.saveJsonData(6, loadRanking);
		//车辆总运行小时top10
		internalScreenService.saveJsonData(7, sumWorkHoursRanking);
		//昨日工作排名(拉运趟次/工作小时)
		internalScreenService.saveJsonData(8, lastDayRanking);
		//国内各省车辆排名
		internalScreenService.saveJsonData(9, carcountByProvince);
		//地区开工饱和度
		internalScreenService.saveJsonData(10, locationWork);
		//海外各国车辆排名
		internalScreenService.saveJsonData(11, carcountByAbroad);
		//车辆位置
		internalScreenService.saveJsonData(12, carLocation);
		//车型月均工作小时
		internalScreenService.saveJsonDataCartype(13,carTypeHourOil);
		//车型月均产量
		internalScreenService.saveJsonDataCartype(14, carTypeMonthOutput);
		//连接阿里云数据库使用
		JdbcUtil jdbcutil = null;
		try {
			param.put("average_work_hours","");
			param.put("driver_type_proportion","");
			param.put("average_vehicle_productivit","");
			param.put("average_ton_vehicle","");
			param.put("average_monthly_work","");
			param.put("average_monthly_output","");
			param.put("vehicl_ranking","");
			param.put("haulage_capacity","");
			param.put("total_hours","");
			param.put("yesterday_work_ranking","");
			param.put("contry_proportion", String.format("%.2f", percentage));
			param.put("large_vehicle_province","");
			param.put("regional_saturation","");
			internalScreenService.saveInternalScreenData(param);
			String sql = "INSERT INTO large_basic (vehicle_intact_rate,vehicle_attendance,mtbf,mttr,sales_vehicle,networked_vehicles,"
					+ "yesterday_totalpull_volum,average_work_hours,driver_type_proportion,average_vehicle_productivit,"
					+ "average_ton_vehicle,average_monthly_work,average_monthly_output,vehicl_ranking,haulage_capacity,total_hours,"
					+ "yesterday_work_ranking,contry_proportion,large_vehicle_province,regional_saturation,updatedatetime)"
					+ " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now())";
			String deleteSql = "TRUNCATE large_basic"; //由于datav只展示第一条记录，所以清除原有的记录，再插入
			jdbcutil = new JdbcUtil(deleteSql);
			jdbcutil.getPst().execute();
			jdbcutil.close();
			jdbcutil = new JdbcUtil(sql);
			PreparedStatement pst = jdbcutil.getPst();
			pst.setString(1, sumAvailability+"");
			pst.setString(2, sumAttendance+"");
			pst.setString(3, averageFlatTime+"");
			pst.setString(4, averageFlatRepairTime+"");
			pst.setString(5, sallCarCount+"");
			pst.setString(6, connectCarCount+"");
			pst.setString(7, String.format("%.2f", lastDayTransportSum));
			pst.setString(8, "");
			pst.setString(9, "");
			pst.setString(10, "");
			pst.setString(11, "");
			pst.setString(12, "");
			pst.setString(13, "");
			pst.setString(14, "");
			pst.setString(15, "");
			pst.setString(16, "");
			pst.setString(17, "");
			pst.setString(18, String.format("%.2f", percentage));
			pst.setString(19, "");
			pst.setString(20, "");
			int executeUpdate = pst.executeUpdate();
			if(executeUpdate>0){
				XxlJobLogger.log("内部大屏统计传送到阿里云数据库成功");
			}else{
				XxlJobLogger.log("内部大屏统计传送到阿里云数据库失败");
			}
			
		}catch(Exception e){
			XxlJobLogger.log("内部大屏统计保存出现异常："+e.getMessage());
		}
		finally {
			if(jdbcutil!=null){
				jdbcutil.close();
			}
		}
		return  SUCCESS;
	}

}
