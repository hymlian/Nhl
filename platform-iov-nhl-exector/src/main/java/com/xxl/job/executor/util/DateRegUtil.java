package com.xxl.job.executor.util;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import net.tycmc.bulb.common.util.DateUtil;
import net.tycmc.bulb.common.util.timeslice.DayTimeSlice;
import net.tycmc.bulb.common.util.timeslice.DayTimeSliceParse;

/**
 * 日期规则类
 * 
 * 
 */
public class DateRegUtil {
	/**
	 * 月份类型
	 */
	private final static String BIG = "big";
	private final static String PING = "ping";
	private final static String SMALL = "small";

	/**
	 * 获取月份开始日期和截止日期 规则：以15号为界限进行月份划分
	 * 
	 * @param year
	 *            年
	 * @param month
	 *            月
	 * @param reportTime
	 *            设定的日期，1~15，开始日期则为本月+reportTime，结束日期为下月+（reportTime-1）；>15，
	 *            开始日期则为上月+reportTime，结束日期为下月+（reportTime-1）。支持跨年。
	 * @return
	 */
	public static String[] getMonthSE(int year, int month, int reportTimeI) {
		String retValue[] = new String[2];
		String reportTime = String.valueOf(reportTimeI);
		// 判断该年是否为闰年，true为闰年
		boolean yearType = judgeYearType(year);
		// 判断该月类型
		String monthType = judgeMonthType(month);
		// 对日前进行判断
		if (reportTimeI == 1) {
			retValue[0] = year + "-" + month + "-1";
			if (monthType.equals(BIG)) {
				retValue[1] = year + "-" + month + "-31";
			} else if (monthType.equals(SMALL)) {
				retValue[1] = year + "-" + month + "-30";
			} else {
				retValue[1] = year + "-" + month + "-28";
				if (yearType)
					retValue[1] = year + "-" + month + "-29";
			}

		} else if (reportTimeI > 1 && reportTimeI <= 15) {
			retValue = getMonthSE_Front(year, String.valueOf(month), reportTime);
		} else {
			if (reportTimeI > 15 && reportTimeI <= 29) {
				if (month == 3) {
					retValue[0] = year + "-" + "2-" + reportTime;
					retValue[1] = year + "-" + "3-"
							+ String.valueOf(Integer.valueOf(reportTime) - 1);
					if (!yearType && reportTimeI == 29)
						retValue[0] = year + "-" + "3-1";
				} else {
					retValue = getMonthSE_After(year, String.valueOf(month),
							reportTime, yearType);
				}
			} else if (reportTimeI == 30) {
				if (month == 2) {
					retValue[0] = year + "-" + "1-30";
					retValue[1] = year + "-" + "2-28";
					if (yearType)
						retValue[1] = year + "-" + "2-29";
				} else if (month == 3) {
					retValue[0] = year + "-" + "3-1";
					retValue[1] = year + "-" + "3-29";
				} else {
					retValue = getMonthSE_After(year, String.valueOf(month),
							reportTime, yearType);
				}
			} else {
				retValue[0] = year + "-";
				retValue[1] = year + "-";
				switch (month) {
				case 1:
					retValue[0] = (year - 1) + "-12-31";
					retValue[1] += "1-30";
					break;
				case 2:
					retValue[0] += "1-31";
					if (!yearType)
						retValue[1] += "2-28";
					if (yearType)
						retValue[1] += "2-29";
					break;
				case 3:
					retValue[0] += "3-1";
					retValue[1] += "3-30";
					break;
				case 4:
					retValue[0] += "3-31";
					retValue[1] += "4-30";
					break;
				case 5:
					retValue[0] += "5-1";
					retValue[1] += "5-30";
					break;
				case 6:
					retValue[0] += "5-31";
					retValue[1] += "6-30";
					break;
				case 7:
					retValue[0] += "7-1";
					retValue[1] += "7-30";
					break;
				case 8:
					retValue[0] += "7-31";
					retValue[1] += "8-30";
					break;
				case 9:
					retValue[0] += "8-31";
					retValue[1] += "9-30";
					break;
				case 10:
					retValue[0] += "10-1";
					retValue[1] += "10-30";
					break;
				case 11:
					retValue[0] += "10-31";
					retValue[1] += "11-30";
					break;
				case 12:
					retValue[0] += "12-1";
					retValue[1] += "12-30";
					break;
				}
			}
		}
		return retValue;
	}

	/**
	 * 年份类型判断,true:闰年，false:非闰年
	 * 
	 * @param year
	 * @return
	 */
	public static boolean judgeYearType(int year) {

		if ((year % 400 == 0) | (year % 4 == 0) & (year % 100 != 0))
			return true;

		return false;
	}

	/**
	 * 判断月份类型
	 * 
	 * @param month
	 * @return
	 */
	public static String judgeMonthType(int month) {
		String monthType = "";
		if (month == 1 || month == 3 || month == 5 || month == 7 || month == 8
				|| month == 10 || month == 12) {
			monthType = BIG;
		} else if (month == 2) {
			monthType = PING;
		} else {
			monthType = SMALL;
		}
		return monthType;
	}

	/**
	 * 获取开始日前小于15号的开始和结束日前
	 * 
	 * @return
	 */
	public static String[] getMonthSE_Front(int year, String month,
			String reportTime) {
		String retValue[] = new String[2];
		String s_month = "";
		String e_month = "";
		if ("12".equals(month)) {
			s_month = month + "";
			e_month = "1";
			retValue[1] = (year + 1) + "-" + e_month + "-"
					+ String.valueOf(Integer.valueOf(reportTime) - 1);
		} else {
			s_month = month;
			e_month = String.valueOf(Integer.valueOf(month) + 1);
			retValue[1] = year + "-" + e_month + "-"
					+ String.valueOf(Integer.valueOf(reportTime) - 1);
		}
		retValue[0] = year + "-" + s_month + "-" + reportTime;
		return retValue;
	}

	/**
	 * 获取开始日前大于15号开始和结束日前
	 * 
	 * @return
	 */
	public static String[] getMonthSE_After(int year, String month,
			String reportTime, boolean yearType) {
		String retValue[] = new String[2];
		String s_month = "";
		String e_month = "";
		if ("1".equals(month)) {
			s_month = "12";
			e_month = month;
			retValue[0] = (year - 1) + "-" + s_month + "-" + reportTime;
		} else {
			s_month = String.valueOf(Integer.valueOf(month) - 1);
			e_month = month;
			retValue[0] = year + "-" + s_month + "-" + reportTime;
		}
		retValue[1] = year + "-" + e_month + "-"
				+ String.valueOf(Integer.valueOf(reportTime) - 1);
		return retValue;
	}

	/**
	 * 时间段
	 * 
	 * @param startTime
	 *            开始时间
	 * @param endTime
	 *            结束时间
	 * @return
	 */
	public static List<String> iterateTime(String startTime, String endTime) {
		List<String> arr = new ArrayList<String>();
		DayTimeSliceParse DTSP = new DayTimeSliceParse();
		List<DayTimeSlice> ls = DTSP.parse(startTime + " 00:00:00", endTime
				+ " 23:59:59");
		Iterator<DayTimeSlice> iter = ls.iterator();
		while (iter.hasNext()) {
			DayTimeSlice dts = iter.next();
			arr.add(DateUtil.toString(dts.getDay(), "yyyy-MM-dd").trim());
		}
		return arr;
	}

	/**
	 * 目标日期是否在指定日期区间内
	 * 
	 * @param startTime
	 * @param endTime
	 * @param targetTime
	 * @return true：在区间内 false：不在区间内
	 */
	public static boolean isContain(String startTime, String endTime,
			String targetTime) {
		try {
			long lStart = DateUtil.toDate(
					startTime,
					(startTime.length() >= 19 ? "yyyy-MM-dd HH:mm:ss"
							: "yyyy-MM-dd")).getTime();
			long lEnd = DateUtil.toDate(
					endTime,
					(endTime.length() >= 19 ? "yyyy-MM-dd HH:mm:ss"
							: "yyyy-MM-dd")).getTime();
			long lTarget = DateUtil.toDate(
					targetTime,
					(targetTime.length() >= 19 ? "yyyy-MM-dd HH:mm:ss"
							: "yyyy-MM-dd")).getTime();

			if (lStart <= lTarget && lTarget <= lEnd)
				return true;

			return false;
		} catch (Exception e) {

		}
		return false;
	}

	/**
	 * 指定日期的所属报表的年月
	 * 
	 * @param nowDate
	 * @param cfgDay
	 * @return 索引0：年 索引1：月
	 */
	public static List<Integer> nowYearMonth(String nowDate, int cfgDay) {
		List<Integer> retVal = null;
		try {
			retVal = new ArrayList<Integer>();
			// 当前日期年月
			int year = Integer.parseInt(DateUtil.toString(
					DateUtil.toDate(nowDate, "yyyy-MM-dd"), "yyyy"));
			int month = Integer.parseInt(DateUtil.toString(
					DateUtil.toDate(nowDate, "yyyy-MM-dd"), "MM"));
			String reportTimes[] = getMonthSE(year, month, cfgDay);
			long lNowDate = DateUtil.toDate(nowDate, "yyyy-MM-dd").getTime();
			long lBNowDate = DateUtil.toDate(reportTimes[0], "yyyy-MM-dd")
					.getTime();
			long lANowDate = DateUtil.toDate(reportTimes[1], "yyyy-MM-dd")
					.getTime();
			if (lNowDate < lBNowDate) {
				String bNowDate = addMonthToTar(
						DateUtil.toDate(nowDate, "yyyy-MM-dd"), -1);
				retVal.add(Integer.valueOf(DateUtil.toString(
						DateUtil.toDate(bNowDate, "yyyy-MM-dd"), "yyyy")));
				retVal.add(Integer.valueOf(DateUtil.toString(
						DateUtil.toDate(bNowDate, "yyyy-MM-dd"), "MM")));
			} else if (lNowDate >= lBNowDate && lNowDate <= lANowDate) {
				retVal.add(Integer.valueOf(year));
				retVal.add(Integer.valueOf(month));
			} else {
				String aNowDate = addMonthToTar(
						DateUtil.toDate(nowDate, "yyyy-MM-dd"), 1);
				retVal.add(Integer.valueOf(DateUtil.toString(
						DateUtil.toDate(aNowDate, "yyyy-MM-dd"), "yyyy")));
				retVal.add(Integer.valueOf(DateUtil.toString(
						DateUtil.toDate(aNowDate, "yyyy-MM-dd"), "MM")));
			}
		} catch (Exception e) {
			return null;
		}
		return retVal;
	}

	/**
	 * 指定日期增加月
	 * 
	 * @param trialTime
	 * @param month
	 * @return
	 */
	public static String addMonthToTar(Date trialTime, int month) {
		SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(trialTime);
		calendar.add(Calendar.MONTH, month);
		return myFormatter.format(calendar.getTime());
	}

	// jiyongtian
	/**
	 * 计算一段时间间隔含有的小时数
	 * 
	 * @param String
	 *            [] monthGap(一段时间间隔开始、结束时间组成的数组)
	 * @return BigDecimal hours(这段时间间隔小时数)
	 */
	public static BigDecimal getHoursOfMonth(String[] monthgap) {
		BigDecimal hours = null;
		int days = DateRegUtil.getDaysOfTimeDef(monthgap);
		hours = new BigDecimal(days).multiply(new BigDecimal("24"));
		return hours;
	}

	/**
	 * 计算一段时间间隔含有的自然天数
	 * 
	 * @param String
	 *            [] monthGap(一段时间间隔开始、结束时间组成的数组)
	 * @return int days(这段时间间隔自然天数)
	 *         当开始时间和结束时间包含时分秒，且相差不到1天，则返回1；若大于1天但小于2天，则返回2；
	 * @author lixiaofan 20140521 修改方法（原因：之前方法逻辑较为复杂）
	 * 
	 */
	public static int getDaysOfTimeDef(String[] monthgap) {
		String smdate = "";
		String bdate = "";
		if (monthgap != null && monthgap.length == 2) {
			if (monthgap[0].length() > 10 && monthgap[1].length() > 10) {
				smdate = monthgap[0];
				bdate = monthgap[1];
			} else if (monthgap[1].length() >= 8) {
				smdate = monthgap[0] + " 00:00:00";
				bdate = monthgap[1] + " 23:59:59";
			} else {
				return -1;
			}
		} else {
			return -1;
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		try {
			cal1.setTime(sdf.parse(smdate));
			cal2.setTime(sdf.parse(bdate));
		} catch (ParseException e) {
			e.printStackTrace();
		}

		long time1 = cal1.getTimeInMillis();
		long time2 = cal2.getTimeInMillis();
		float between_days = (float) (time2 - time1) / (1000 * 3600 * 24);
		int days = (int) (between_days);
		return Integer.parseInt(String.valueOf(days < between_days ? days + 1
				: days));
	}

	/**
	 * 返回一个自然月的天数
	 * 
	 * @param int year(年)
	 * @param int month(月)
	 * @return int days(天数)
	 */
	public static int getDaysOfMonth(int year, int month) {
		int days = 0;
		int[] daysArray = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
		if (year % 400 == 0 || (year % 4 == 0 && year % 100 != 0)) {
			daysArray[1] = 29;
		}
		days = daysArray[month - 1];
		return days;
	}

	// end
	/**
	 * 将制定时间间隔重新分成更小的连续时间段（按天数）， 从较早时间开始，最后不足指定天数按照一段处理,紧连的两段重复一天
	 * 
	 * @param String
	 *            startTime 开始时间
	 * @param String
	 *            endtime 结束时间
	 * @param int dateDef 指定天数
	 **/
	public static List<String[]> toArrayOfTimingDef(String startTime,
			String endTime, int dateDef) {
		List<String[]> reList = new ArrayList<String[]>();
		SimpleDateFormat sdf0 = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date start_date = sdf0.parse(startTime);
			Date end_date = sdf0.parse(endTime);
			Calendar ca = Calendar.getInstance();
			ca.setTime(start_date);
			String[] small = new String[2];
			small[0] = startTime;
			int k = 1;
			while (ca.getTime().getTime() <= end_date.getTime()) {
				if (k == dateDef && ca.getTime().getTime() < end_date.getTime()) {
					k = 1;
					small[1] = sdf0.format(ca.getTime());
					reList.add(small);
					small = new String[2];
					small[0] = sdf0.format(ca.getTime());
				} else if (ca.getTime().getTime() == end_date.getTime()) {
					small[1] = endTime;
					reList.add(small);
				}
				k++;
				ca.add(Calendar.DAY_OF_YEAR, 1);
			}
		} catch (ParseException e) {
			// e.printStackTrace();
		}
		return reList;
	}

	/**
	 * 比较两个时间的大小 如 date1 大于date2 返回 true，否则 返回false 异常时返回false
	 * 
	 * @param String
	 *            date1
	 * @param String
	 *            date2
	 * @param String
	 *            format
	 **/
	public static boolean compare(String date1, String date2, String format) {
		boolean flag = false;
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			Date start = sdf.parse(date1);
			Date end = sdf.parse(date2);
			if (start.getTime() < end.getTime()) {

			} else {
				flag = true;
			}
		} catch (ParseException e) {
			// e.printStackTrace();
		}
		return flag;
	}

	/**
	 * sxh 2014.2.5 一段时间内的自然月，最多可以隔1年 不支持2014~2016这样相隔2年的 录入‘2014-10-01
	 * 11:10:10’~‘2015-05-01 10:11:11’ 返回int
	 * 数组{201410,201411,201412,201501,201502....201505}
	 */
	public static int[] OldgetMonthSql(String startTime, String endTime) {
		startTime = DateUtil.addDay(startTime, 0);// 为了将传递的时间“2015-5-1”变为“2015-05-01”
		endTime = DateUtil.addDay(endTime, 0);
		String bgnyear = startTime.substring(0, 4);
		String endyear = endTime.substring(0, 4);
		if (bgnyear.equals(endyear)) {// 年份相同，不跨年
			String startMonth = startTime.substring(0, 7).replace("-", "");
			String endMonth = endTime.substring(0, 7).replace("-", "");
			int sm = Integer.parseInt(startMonth);
			int em = Integer.parseInt(endMonth);
			int[] months = new int[em - sm + 1];
			int j = 0;
			for (int i = sm; i <= em; i++) {
				months[j] = sm + j;
				j++;
			}
			return months;
		} else {// 跨年，仅仅相隔一年(目前逻辑如此)
			int j = 0;
			int startMonth = Integer.parseInt(startTime.substring(5, 7));
			int endMonth = Integer.parseInt(endTime.substring(5, 7));
			int chayue = endMonth + 12 - startMonth + 1;// 时间内包含几个月，包括开始月和结束月
			int[] months = new int[chayue];
			int startYear = Integer.parseInt(bgnyear.substring(0, 4));
			int endYear = Integer.parseInt(endyear.substring(0, 4));
			for (int i = startMonth; i <= endMonth + 12; i++) {
				if (i <= 12) {
					months[j] = startYear * 100 + i;
					j++;
				} else {
					months[j] = endYear * 100 + i - 12;
					j++;
				}
			}
			return months;
		}

	}

	/**
	 * 获取时间段内的每天日期集合
	 * 
	 * @param begintime
	 *            ,endtime
	 * @return List<String>
	 * */
	public static List<String> getDateList(String begintime, String endtime) {
		Date dBegin = DateUtil.toDate(begintime, "yyyy-MM-dd");
		Date dEnd = DateUtil.toDate(endtime, "yyyy-MM-dd");
		List<String> lDate = new ArrayList<String>();
		lDate.add(DateUtil.toString(dBegin, "yyyy-MM-dd"));
		Calendar calBegin = Calendar.getInstance();
		// 使用给定的 Date 设置此 Calendar 的时间
		calBegin.setTime(dBegin);
		Calendar calEnd = Calendar.getInstance();
		// 使用给定的 Date 设置此 Calendar 的时间
		calEnd.setTime(dEnd);
		// 测试此日期是否在指定日期之后
		while (dEnd.after(calBegin.getTime())) {
			// 根据日历的规则，为给定的日历字段添加或减去指定的时间量
			calBegin.add(Calendar.DAY_OF_MONTH, 1);
			lDate.add(DateUtil.toString(calBegin.getTime(), "yyyy-MM-dd"));
		}
		return lDate;
	}

	public static String[] getMonthSql(String startTime, String endTime) {
		startTime = DateUtil.addDay(startTime, 0);// 为了将传递的时间“2015-5-1”变为“2015-05-01”
		endTime = DateUtil.addDay(endTime, 0);
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(DateUtil.toDate(startTime, "yyyy-MM"));
		Calendar cal2 = Calendar.getInstance();
		cal2.setTime(DateUtil.toDate(endTime, "yyyy-MM"));
		cal2.add(Calendar.MONTH, 1);
		List<String> list = new ArrayList<String>();
		while (cal2.getTime().after(cal1.getTime())) {
			list.add(DateUtil.toString(cal1.getTime(), "yyyy-MM"));
			cal1.add(Calendar.MONTH, 1);
		}
		String[] month = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			month[i] = list.get(i).replaceAll("-", "");
		}
		return month;
	}

	/**
	 * 得到同一天中，两个时间中每个小时
	 * 
	 * @param beginTime
	 * @param endTime
	 * @return
	 */
	public static List<String> GetEveryHours(String beginTime, String endTime) {
		if (beginTime.length() < 10) {// 针对用户二次修改保证时间正确
			beginTime = DateUtil.addDay(beginTime, 0);
		}
		beginTime = clearSpecialCharacter(beginTime, 1);
		endTime = clearSpecialCharacter(endTime, 2);
		List<String> hourList = new ArrayList<String>();
		int begin = Integer.valueOf(beginTime);
		int end = Integer.valueOf(endTime);
		while (begin <= end) {
			hourList.add(begin + "");
			begin++;
		}
		return hourList;
	}

	/**
	 * 清理特殊字符得到时间格式2016083001，只精确到小时
	 * 
	 * @param s传入的时间
	 *            ，i格式——1、开始时间；2、结束时间；0、不操作
	 * @return
	 */
	public static String clearSpecialCharacter(String s, int i) {
		s = s.replace("-", "");
		s = s.replace(" ", "");
		if (1 == i) {
			while (s.length() <= 10) {
				s = s + "0";
			}
		} else if (2 == i) {
			if (s.length() <= 8) {
				s = DateUtil.toString(DateUtil.now(), "yyyyMMddHH");
			}
		}
		s = s.substring(0, 10);
		return s;
	}

	/**
	 * 得到今天的日期20160830
	 * 
	 * @return
	 */
	public static String GetToday() {
		// 今天日期文件夹如：20160830
		DateFormat date = DateFormat.getDateInstance();
		String today = date.format(new Date());
		today = DateUtil.addDay(today, 0);
		today = today.replace("-", "");
		return today;
	}

	/**
	 * 传入类型标志符 得到日期
	 * 
	 * @return
	 * @throws ParseException 
	 */
	public static String GetTime(int i){
		DateFormat dateform = DateFormat.getDateInstance();
		Date date = new Date();
		String today = dateform.format(date);
		
		switch (i) {
		case 1: // 今天日期如：2016-08-30
			break;
		case 2: // 今天日期如：20160830
			today = DateUtil.addDay(today, 0);
			today = today.replace("-", "");
			break;
		case 3: // 当月如：201608
			today = DateUtil.addDay(today, 0);
			today = today.replace("-", "").substring(0, 6);
			break;
		case 4: // 时间戳如：2017-05-06 08:36:23
			// today = DateUtil.toString(DateUtil.now());
			today = DateUtil.addDay(today, 0);
			today = DateUtil.toString(date);
			break;
		case 5: // 昨天日期如：20160830
			today = DateUtil.addDay(today, -1); 
			today = today.replace("-", "");
			break;
		case 6: // 今天日期(去掉0)如：2016-8-30
			today = DateUtil.addDay(today, 0);
			SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-M-d");
			today = myFormatter.format(date);
			break;
		case 7: // 第二天日期如：20160830
			today = DateUtil.addDay(today, 1);
			today = today.replace("-", "");
			break;
			
		default:
			break;
		}
		return today;
	}

	/**
	 * 通过时间戳得到时间
	 * 
	 * @param ltime
	 * @return
	 */
	public static String GetTimeByTimesTamp(long ltime) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String today = format.format(ltime);

		return today;
	}

	/**
	 * 通过时间得到时间戳
	 * 
	 * @param ltime
	 * @return
	 */
	public static long GetTimesTampByLTime(String Time) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = format.parse(Time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date.getTime();
	}
	/**
	 * 通过时间得到时间戳
	 * 
	 * @param ltime
	 * @return
	 */
	public static long GetTimes(String Time) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date date = null;
		try {
			date = format.parse(Time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date.getTime();
	}

	/**
	 * 通过定时表模板得到当前时间的定时表
	 * 
	 * @param DBName
	 * @param TableName
	 * @return
	 */
	public static String GetTodayTable(String DBName, String TableName) {
		return DBName.replaceAll("yyyymm", GetTime(3)) + ".dbo."
				+ TableName.replaceAll("yyyymmdd", GetTime(2));
	}
	
	/**
	 * 大于当前时间返回true
	 * @param UsefulLife
	 * @return
	 */
	public static boolean CompareWithNow(String UsefulLife) {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			long UsefulLifeTime=sdf.parse(UsefulLife).getTime();
			long nowTime=new Date().getTime();
			if(UsefulLifeTime>nowTime){
				return true;
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public static String ConvertStrDate(long ltime){
		Date date=new Date(ltime);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return format.format(date);
	}
	/**
	 *方法说明：获得本周的 周一 日期
	 *<br/>参数：Date date
	 *<br/>返回值：本周的 周一 日期
	 */
	public static String getMonday(Date date){
		   int[] weekDays = {7, 1, 2, 3, 4, 5, 6};
	        Calendar cal = Calendar.getInstance();
	        cal.setTime(date);
	        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
	        if (w < 0)
	            w = 0;
	        int week=weekDays[w];
	        //获取周一日期
	        return addDay(new SimpleDateFormat("yyyy-MM-dd").format(date),1-week);
	}
	/**
	 *方法说明：获得本周的 周日 日期
	 *<br/>参数：Date date
	 *<br/>返回值：本周的 周日 日期
	 *精确油耗统计用到
	 */
	public static String getSundayS(Date date){
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd"); 
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int dayWeek=c.get(Calendar.DAY_OF_WEEK);
		if(1==dayWeek){
			c.add(Calendar.DAY_OF_MONTH, -1);
		}
		c.setFirstDayOfWeek(Calendar.MONDAY);
		int day=c.get(Calendar.DAY_OF_WEEK);
		c.add(Calendar.DATE,c.getFirstDayOfWeek()-day);
		c.add(Calendar.DATE, 6);
		String sunday=formatter.format(c.getTime());
		return sunday;
	}
	public static String addDay(String date, int days) {
		SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = new GregorianCalendar();
		Date trialTime = new Date();
		try {
			trialTime = myFormatter.parse(date);
		} catch (ParseException e) {
		}
		calendar.setTime(trialTime);
		calendar.add(Calendar.DAY_OF_MONTH, days);
		return myFormatter.format(calendar.getTime());
	}
	/**
	 *方法说明：日期所在月的第一天
	 *<br/>参数：Date date
	 *<br/>返回值：本周的 周一 日期
	 */
	public static String getfirstDay(Date date){
		   Calendar c = Calendar.getInstance();
		   c.setTime(date);
		   c.set(Calendar.DAY_OF_MONTH,1);
		  // c.add(Calendar.DAY_OF_MONTH,-1);
		   return new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
	}
	/**
	 *方法说明：日期所在月的上个月最后一天
	 *<br/>参数：Date date
	 *<br/>返回值：本周的 周一 日期
	 */
	public static String getendDay(Date date){
		   Calendar c = Calendar.getInstance();
		   c.setTime(date);
		   c.set(Calendar.DAY_OF_MONTH,0);
		  // c.add(Calendar.DAY_OF_MONTH,-1);
		   return new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
	}
	/**
	 * 获取指定日期的前7天日期
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
	/**
	 * 获取指定日期的前7天日期
	 * @param day
	 * @return yyyyMMdd
	 */
	public static String sevenDay(String day){
		Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd").parse(day);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day1 = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day1 - 6);
        String dayAfter = new SimpleDateFormat("yyyyMMdd").format(c.getTime());
		return dayAfter;
	}
	/**
	 * 获取指定日期的前7天日期
	 * @param day
	 * @return yyyy-MM-dd
	 */
	public static String sevenDay2(String day){
		Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd").parse(day);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day1 = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day1 - 6);
        String dayAfter = new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
		return dayAfter;
	}
	/**
	 * 获取指定日期的前一月日期
	 * @param day
	 * @return yyyy-MM-dd
	 */
	public static String yueDay(String day){
		Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd").parse(day);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day1 = c.get(Calendar.MONTH);
        c.set(Calendar.MONTH, day1 - 1);
        String dayAfter = new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
		return dayAfter;
	}
	/**
	 * 2018-09-01,2018-09-30返回9.1-9.30
	 * @param firstDay
	 * @param BeginTime
	 * @return
	 */
	public static String riqi(String firstDay,String BeginTime){
		String yuef1 = firstDay.substring(5, 7);
		String yuef2 = BeginTime.substring(5, 7);
		String b1 = yuef1.substring(0,1);
		String b2 = yuef2.substring(0,1);
		if("0".equals(b1)){
			yuef1=yuef1.substring(1,2);
		}
		if("0".equals(b2)){
			yuef2=yuef2.substring(1,2);
		}
		String r1 = firstDay.substring(8, 10);
		String r2 = BeginTime.substring(8, 10);
		String b3 = r1.substring(0,1);
		String b4 = r2.substring(0,1);
		if("0".equals(b3)){
			r1=r1.substring(1,2);
		}
		if("0".equals(b4)){
			r2=r2.substring(1,2);
		}
		String rq1 = yuef1+"."+r1+"-"+yuef2+"."+r2;
		return rq1;
	}
	/**
	 * 2018-09-01,2018-09-30返回9.1-9.30
	 * @param firstDay
	 * @param BeginTime
	 * @return
	 */
	public static String zhouqi(String monday,String sundayS){
		String yuefen1 = monday.substring(5, 7);
		String yuefen2 = sundayS.substring(5, 7);
		String a1 = yuefen1.substring(0,1);
		String a2 = yuefen2.substring(0,1);
		if("0".equals(a1)){
			yuefen1=yuefen1.substring(1,2);
		}
		if("0".equals(a2)){
			yuefen2=yuefen2.substring(1,2);
		}
		String ri1 = monday.substring(8, 10);
		String ri2 = sundayS.substring(8, 10);
		String a3 = ri1.substring(0,1);
		String a4 = ri2.substring(0,1);
		if("0".equals(a3)){
			ri1=ri1.substring(1,2);
		}
		if("0".equals(a4)){
			ri2=ri2.substring(1,2);
		}
		String riqi1 = yuefen1+"."+ri1+"-"+yuefen2+"."+ri2;
		return riqi1;
	}
	/**
	 * 获取上个月的最后一天
	 */
	public static String lastMonthDay(String CurrentDate){
		Date date2 = DateUtil.toDate(CurrentDate, "yyyy-MM-dd");
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		//获取当前日期
		Calendar instance = Calendar.getInstance();
		instance.setTime(date2);
		instance.set(Calendar.DAY_OF_MONTH,0);
		String date = format.format(instance.getTime());
		return date;
	}
	/**
	 * 获取去年的本月日期
	 * @param args
	 */
	public static String lastYearDay(String CurrentDate){
		Date date2 = DateUtil.toDate(CurrentDate, "yyyy-MM-dd");
		SimpleDateFormat format = new SimpleDateFormat("yyyyMM");
		//获取当前日期
		Calendar instance = Calendar.getInstance();
		instance.setTime(date2);
		instance.add(Calendar.YEAR, -1);
		String date = format.format(instance.getTime());
		return date;
	}
	/**
	 * 获取上个月的年月 格式201806
	 * @param args
	 * @throws ParseException
	 */
	public static String lastYearAndMonth(String CurrentDate){
		Date date2 = DateUtil.toDate(CurrentDate, "yyyy-MM-dd");
		SimpleDateFormat format = new SimpleDateFormat("yyyyMM");
		//获取当前日期
		Calendar instance = Calendar.getInstance();
		instance.setTime(date2);
		instance.add(Calendar.MONDAY, -1);
		String date = format.format(instance.getTime());
		return date;
	}
	/**
	 * 获取本月的年月 格式201806
	 * @param args
	 * @throws ParseException
	 */
	public static String lastCurrentYearAndMonth(String CurrentDate){
		Date date2 = DateUtil.toDate(CurrentDate, "yyyy-MM-dd");
		SimpleDateFormat format = new SimpleDateFormat("yyyyMM");
		//获取当前日期
		Calendar instance = Calendar.getInstance();
		instance.setTime(date2);
		String date = format.format(instance.getTime());
		return date;
	}
	/**
	 * 获取昨天的日期
	 * @param args
	 * @throws ParseException
	 */
	public static String getLastDay(String CurrentDate){
		Date date2 = DateUtil.toDate(CurrentDate, "yyyy-MM-dd");
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		//获取当前日期
		Calendar instance = Calendar.getInstance();
		instance.setTime(date2);
		instance.add(Calendar.DATE, -1);
		String date = format.format(instance.getTime());
		return date;
	}
	/**
	 *方法说明：日期所在月的第一天
	 *<br/>参数：Date date
	 *<br/>返回值：本周的 周一 日期
	 */
	public static String getfirstDay(String day){
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date date = format.parse(day);
			  Calendar c = Calendar.getInstance();
			   c.setTime(date);
			   c.set(Calendar.DAY_OF_MONTH,1);
			  // c.add(Calendar.DAY_OF_MONTH,-1);
			   return format.format(c.getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return null;
	}
	/**
	 *方法说明：日期所在月的上个月最后一天
	 *<br/>参数：Date date
	 *<br/>返回值：本周的 周一 日期
	 */
	public static String getendDay(String day){
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date date = format.parse(day);
			  Calendar c = Calendar.getInstance();
			   c.setTime(date);
			   //c.set(Calendar.DAY_OF_MONTH,0);//上月最后一天
			   c.set(Calendar.DAY_OF_MONTH,c.getActualMaximum(Calendar.DAY_OF_MONTH));//当月最后一天
			   return format.format(c.getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return null;
	}
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws ParseException {
		System.out.println(getfirstDay("2018-10-02"));
		System.out.println(getendDay("2000-02-10"));
	}
}
