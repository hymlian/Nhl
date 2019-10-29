package com.xxl.job.executor.jobhandler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.service.BigScreenOverViewService;


@JobHandler(value="BigScreenOverViewHandler")
@Component
public class BigScreenOverViewHandler extends IJobHandler{

	@Autowired
	private BigScreenOverViewService bigScreenOverViewService;
	
	@Override
	public ReturnT<String> execute(String arg0) throws Exception {
		// TODO Auto-generated method stub
		XxlJobLogger.log("大屏展示统计开始：");
		boolean flag = bigScreenOverViewService.dealWithBigScreenData();
		return SUCCESS;
	}

}
