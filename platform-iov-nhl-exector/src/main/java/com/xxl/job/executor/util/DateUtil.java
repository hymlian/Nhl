package com.xxl.job.executor.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

public class DateUtil {
	/**
	 * 方法说明:取得当前日期格式
	 * <br/>参数:i=0,结果 yyyy-MM-dd HH:mm:ss
	 * <br/>参数:i=1,结果 yyyy-MM-dd
	 * <br/>参数:i=2,结果 yyyy年MM月dd日 HH:mm:ss
	 * <br/>参数:i=3,结果 yyyy年MM月dd日
	 * <br/>参数:i=4,结果 yyyy-MM
	 * <br/>返回日期：
	 */
	public static String getDateTime(int i) {
		String nowdate = null;
		String Type = "yyyy-MM-dd HH:mm:ss";
		if(i==0){
			Type = "yyyy-MM-dd HH:mm:ss";
		}else if(i==1){
			Type = "yyyy-MM-dd";
		}else if(i==2){
			Type = "yyyy年MM月dd日 HH:mm:ss";
		}else if(i==3){
			Type = "yyyy年MM月dd日";
		}else if(i==4){
			Type = "yyyy-MM";
		}else if(i==5){
			Type = "yyyyMMdd";
		}else if(i==6){
			Type = "yyyyMMddHHmmss";
		}else if(i==7){
			Type = "yyyyMM";
		}

		
		SimpleDateFormat formatter = new SimpleDateFormat(Type); 
		Date date=new Date();
		nowdate = formatter.format(date);	
		return nowdate;
	}
	
	public static String getDateTime(Date date,int i) {
		String nowdate = null;
		String Type = "yyyy-MM-dd HH:mm:ss";
		if(i==0){
			Type = "yyyy-MM-dd HH:mm:ss";
		}else if(i==1){
			Type = "yyyy-MM-dd";
		}else if(i==2){
			Type = "yyyy年MM月dd日 HH:mm:ss";
		}else if(i==3){
			Type = "yyyy年MM月dd日";
		}else if(i==4){
			Type = "yyyy-MM";
		}else if(i==5){
			Type = "yyyyMMdd";
		}else if(i==6){
			Type = "yyyyMMddHHmmss";
		}else if(i==7){
			Type = "yyyyMM";
		}

		
		SimpleDateFormat formatter = new SimpleDateFormat(Type); 
		nowdate = formatter.format(date);	
		return nowdate;
	}
	
	/**
	 * 得到指定日期
	 * @param date
	 * @param i
	 * @return
	 */
	public static String addDay(int day,int i) {
		String nowdate = null;
		String Type = "yyyy-MM-dd HH:mm:ss";
		if(i==0){
			Type = "yyyy-MM-dd HH:mm:ss";
		}else if(i==1){
			Type = "yyyy-MM-dd";
		}else if(i==2){
			Type = "yyyy年MM月dd日 HH:mm:ss";
		}else if(i==3){
			Type = "yyyy年MM月dd日";
		}else if(i==4){
			Type = "yyyy-MM";
		}else if(i==5){
			Type = "yyyyMMdd";
		}else if(i==6){
			Type = "yyyyMMddHHmmss";
		}else if(i==7){
			Type = "yyyyMM";
		}

		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat(Type);
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.DATE, day);
		nowdate = formatter.format(calendar.getTime());	
		return nowdate;
	}
	public static String addMinite(int minite,long time) {
		String nowdate = null;
		String Type = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat formatter = new SimpleDateFormat(Type);
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(new Date(time));
		calendar.add(Calendar.MINUTE, minite);
		nowdate = formatter.format(calendar.getTime());	
		return nowdate;
	}
	public static long addMinites(int minite,long time) {
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(new Date(time));
		calendar.add(Calendar.MINUTE, minite);
		return calendar.getTime().getTime();
	}
	/**
	 * 获取当前时间往前倒多少个月，的月份集合
	 * 
	 * @param int
	 * @return String[]
	 * */
	public static String[] getMonthList(int length) {
		String[] objMonth = new String[length];
		objMonth[0] = getDateTime(new Date(),7);
		Date dBegin = new Date();
		Calendar calBegin = Calendar.getInstance();
		// 使用给定的 Date 设置此 Calendar 的时间
		calBegin.setTime(dBegin);
		for(int i=1;i<length;i++) {
			// 根据日历的规则，为给定的日历字段添加或减去指定的时间量
			calBegin.add(Calendar.MONTH, -1);
			objMonth[i] = DateUtil.getDateTime(calBegin.getTime(), 7);
		}
		return objMonth;
	}
	
	public static long getHowLong(String t1,String t2,int type) throws ParseException{
		//type=1,秒数，type=2,分钟，type=3,小时，type=4,天数
		double result = 0;
		Date date1 = new Date();
		Date date2 = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		date1 = format.parse(t1);
		date2 = format.parse(t2);
		double time = 0;
		if(date2.after(date1)){
			time = date2.getTime()-date1.getTime();
			System.out.println(time);
			System.out.println(time/(60*60*1000));
		}else{
			time = date1.getTime()-date2.getTime();
		}
		if(type==1){
			result = time/1000;
		}else if(type==2){
			result = time/(60*1000);
		}else if(type==3){
			result = time/(60*60*1000);
		}else if(type==4){
			result = time/(24*60*60*1000);
		}else{
			result = 0.00;
		}
		return Math.round(result);
	}
	public static long geLongTime(String time) {
		String Type = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf=new SimpleDateFormat(Type);
		try {
			return sdf.parse(time).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
		}
	public static void main(String[] args) {
		//System.out.println(getDateTime(new Date(),1));
		System.out.println(addDay(0,7));
	}
	
	/**
	 * 获取指定日期的前一天日期
	 * @param day
	 * @return yyyyMMdd
	 */
	public static String oneDay(String day){
		Calendar c = Calendar.getInstance();
        
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd").parse(day);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day1 = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day1 - 1);
 
        String dayAfter = new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
		return dayAfter;
	}
}
