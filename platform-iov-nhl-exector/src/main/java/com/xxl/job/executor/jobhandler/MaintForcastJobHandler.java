package com.xxl.job.executor.jobhandler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.service.MaintAlertService;
import com.xxl.job.executor.util.ListUtil;

import net.tycmc.bulb.common.util.DateUtil;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * 任务Handler示例（Bean模式）
 *
 * 开发步骤：
 * 1、继承"IJobHandler"：“com.xxl.job.core.handler.IJobHandler”；
 * 2、注册到Spring容器：添加“@Component”注解，被Spring容器扫描为Bean实例；
 * 3、注册到执行器工厂：添加“@JobHandler(value="自定义jobhandler名称")”注解，注解value值对应的是调度中心新建任务的JobHandler属性的值。
 * 4、执行日志：需要通过 "XxlJobLogger.log" 打印执行日志；
 *
 * @author xuxueli 2015-12-19 19:43:36
 */
@JobHandler(value="MaintForcastJobHandler")
@Component
public class MaintForcastJobHandler extends IJobHandler {
	@Autowired
	private MaintAlertService maintAlertService;
	
	/*
	 * 1:东区
	 * param  1,0,6
	 *  (non-Javadoc)
	 * @see com.xxl.job.core.handler.IJobHandler#execute(java.lang.String)
	 */
	@Override
	public ReturnT<String> execute(String param) throws Exception {
		XxlJobLogger.log("保养预测统计开始:"+param);
		String dateStr=DateUtil.addDay(-1);
		Map<String,Object>paramRe=new HashMap<String,Object>();
		paramRe.put("condition",param);
		List<Map<String,Object>>listresult=maintAlertService.getLastMaintAlertPerVcl(paramRe);
		//获取每台车的累积工作时长
		List<Map<String,Object>>listWorkHour=maintAlertService.getLJWorkHourPerVcl( dateStr);
		Map<String,List<Map<String,Object>>>MapWorkHour=new ListUtil().groupByOrder("PfVehicleId", listWorkHour);
		//获取每台车不同规则的保养提醒信息
		List<Map<String,Object>>listPerRuler=maintAlertService.getMaintInfoPerVclDiffRuler();
		Map<String,List<Map<String,Object>>>mapPerRuler=new ListUtil().groupByOrder("pfvehicleid", listPerRuler);
		//获取近30天的平均工作时长
		List<Map<String,Object>>list30DayAvgWorkHour=maintAlertService.getLast30DayAvageWorkHour();
		Map<String,List<Map<String,Object>>>map30DayAvgWorkHour=new ListUtil().groupByOrder("MsgESSLT_VclID", list30DayAvgWorkHour);
		if(listresult!=null&&listresult.size()>0){
			List<Future<String>>futures=new ArrayList<Future<String>>();
			List<String>sqlList=new ArrayList<String>();
  			for(Map<String,Object>mapresult:listresult){
				String PfVehicleid=MapUtils.getString(mapresult, "PfVehicleid");
				String ma_vclid=MapUtils.getString(mapresult, "ma_vclid");
				String mri_ma_id=MapUtils.getString(mapresult, "mri_ma_id");
				mapresult.put("dateStr", dateStr);
				if(StringUtils.isNotBlank(ma_vclid)){//说明存在最新一条保养提醒
					//判断是否被保养了
					if(StringUtils.isNotBlank(mri_ma_id)){//存在从键，说明被保养了，继续判断保养逻辑
						Future<String> future=maintAlertService.execMaintForcastService(mapresult,MapWorkHour.get(PfVehicleid) , mapPerRuler.get(PfVehicleid),map30DayAvgWorkHour.get(PfVehicleid));
						futures.add(future);
					}else{//最新一条未被保养，不考虑保养逻辑
						continue;
					}
				}else{//说明不存在最新的一条，继续判断保养逻辑
					Future<String> future=maintAlertService.execMaintForcastService(mapresult,MapWorkHour.get(PfVehicleid) , mapPerRuler.get(PfVehicleid),map30DayAvgWorkHour.get(PfVehicleid));
					futures.add(future);
				}
			}
			for(Future<String> futureRe:futures){
				String sql=futureRe.get();
				if(StringUtils.isNotBlank(sql)){
					sqlList.add(sql);
				}
			} 
			//判断sqllist的size
			if(sqlList.size()>0){
				//开始执行批量插入
				boolean flag=maintAlertService.execBatch(sqlList);
				if(true==flag){
					XxlJobLogger.log("执行成功，本次成功统计的sql条数为："+sqlList.size());
				}else{
					XxlJobLogger.log("执行失败，本次应统计的sql条数为："+sqlList.size());
				}
			}else{
				XxlJobLogger.log("执行成功，本次成功统计的sql条数为："+0);
			}
		}
		return SUCCESS;
	}

}
