package com.xxl.job.executor.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.xxl.job.executor.dao.MaintAlertListDao;

@Component
public class MaintAlertListService {
	@Autowired
	private MaintAlertListDao maintAlertListDao;
	
	public List<Map<String,Object>> getmesList(String oneDay,String biao){
		return maintAlertListDao.getmesList(oneDay,biao);
	}
	
	public Map<String,Object> getmaintrulerList(int VehicleTypeId){
		return maintAlertListDao.getmaintrulerList(VehicleTypeId);
	}
	
	public List<Map<String,Object>> maintrulerdetailList(int mrId){
		return maintAlertListDao.maintrulerdetailList(mrId);
	}
	
	public List<Map<String,Object>> maintalertList(){
		return maintAlertListDao.maintalertList();
	}
}
