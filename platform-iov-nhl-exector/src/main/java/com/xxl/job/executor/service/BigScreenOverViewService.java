package com.xxl.job.executor.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.xxl.job.executor.dao.BigScreenOverViewDao;

@Component
public class BigScreenOverViewService {
	@Autowired
	private BigScreenOverViewDao bigScreenOverViewDao;
	
	public boolean dealWithBigScreenData(){
		bigScreenOverViewDao.dealWithBigScreenData();
		return true;
	}
}
