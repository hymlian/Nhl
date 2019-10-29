package com.xxl.job.executor.jobhandler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.service.MaintAlertService;
import com.xxl.job.executor.service.VclDayService;
import com.xxl.job.executor.util.DateRegUtil;
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
 *车天统计作业
 * @author xuxueli 2015-12-19 19:43:36
 */
@JobHandler(value="VclDayJobHandler")
@Component
public class VclDayJobHandler extends IJobHandler {
	@Autowired
	private VclDayService vclDayService;
	
	/*
	 * 1:东区
	 * param  1,0,6
	 *  (non-Javadoc)
	 * @see com.xxl.job.core.handler.IJobHandler#execute(java.lang.String)
	 */
	@Override
	public ReturnT<String> execute(String param) throws Exception {
		//String staticDay=
		XxlJobLogger.log("车天数据统计开始"+param);
		String []str=param.split(",");
		if(str.length>1){
			vclDayService.getExecVcl(str[0], str[1]);
		}else{
			String date=DateUtil.addDay(-1);
			vclDayService.getExecVcl(param, date);
		}
		XxlJobLogger.log("车天数据统计结束");
		return SUCCESS;
	}

}
