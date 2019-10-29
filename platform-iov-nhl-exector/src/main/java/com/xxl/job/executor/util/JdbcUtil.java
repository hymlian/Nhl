package com.xxl.job.executor.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class JdbcUtil {
	//测试使用
/*	private  String aliyunUrl ="jdbc:mysql://192.168.30.101:3306/CTY_NHL?characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=no";
	private  String aliyunClassName ="com.mysql.jdbc.Driver";
	private  String  aliyunUser="root";
	private  String  aliyunPassword ="tykj66TYKJ";*/
	//阿里云
	private  String aliyunUrl ="jdbc:mysql://rm-2ze3xf88c9op0n0v2io.mysql.rds.aliyuncs.com:3306/cty_datav?characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=no";
	private  String aliyunClassName ="com.mysql.jdbc.Driver";
	private  String  aliyunUser="kjcty";
	private  String  aliyunPassword ="tykj66TYKJ";

	private  Connection connection = null;
	private PreparedStatement pst = null;
	public JdbcUtil(String sql){
		try {
			Class.forName(aliyunClassName);
			connection = DriverManager.getConnection(aliyunUrl,aliyunUser,aliyunPassword);
			pst = connection.prepareStatement(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public   void close (){
		try {
			if(null!=this.pst){
				this.pst.close();
			}
			if(null!=this.connection){
				this.connection.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public Connection getConnection() {
		return connection;
	}
	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	public PreparedStatement getPst() {
		return pst;
	}
	public void setPst(PreparedStatement pst) {
		this.pst = pst;
	}
}
