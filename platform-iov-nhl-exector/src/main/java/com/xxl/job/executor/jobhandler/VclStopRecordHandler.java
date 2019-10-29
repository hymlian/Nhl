package com.xxl.job.executor.jobhandler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import com.xxl.job.executor.service.VclStopRecordService;


@JobHandler(value="VclStopRecordHandler")
@Component
public class VclStopRecordHandler extends IJobHandler{

	@Autowired
	private VclStopRecordService vclStopRecordService;
	
	@Override
	public ReturnT<String> execute(String arg0) throws Exception {
		// TODO Auto-generated method stub
		XxlJobLogger.log("日工作时间分布表统计开始 ");
		vclStopRecordService.VclStopRecord();
		return SUCCESS;
	}

}
