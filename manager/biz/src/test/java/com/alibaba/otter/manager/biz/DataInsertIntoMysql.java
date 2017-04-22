package com.alibaba.otter.manager.biz;

import java.sql.DriverManager;
import java.util.Random;

public class DataInsertIntoMysql {

	public static String getRandomString(int length) { // length表示生成字符串的长度
		String base = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
		Random random = new Random();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < length; i++) {
			int number = random.nextInt(base.length());
			sb.append(base.charAt(number));
		}
		return sb.toString();
	}

	public static final String url = "jdbc:mysql://172.16.28.92/otter_test";
	public static final String name = "com.mysql.jdbc.Driver";
	public static final String user = "otter";
	public static final String password = "otter";

	public static java.sql.Connection conn = null;
	public static java.sql.PreparedStatement pst = null;

	public static void main(String args[]) {

		try {
			Class.forName(name);
			conn = DriverManager.getConnection(url, user, password);// 获取连接
			conn.setAutoCommit(false);
		} catch (Exception e1) {
			e1.printStackTrace();
		} // 指定连接类型

		Random random = new Random();
		// int k = random.nextInt();
		// System.out.println(k);
		int x = 0;
		try {
			String sql = "insert into infoagetime(prod_name,prod_id,ods_date) values(?,?,?)";
			// System.out.println(sql);
			pst = conn.prepareStatement(sql);

			while (x < 5000000) {
				
				pst.setString(1, getRandomString(10));
				pst.setInt(2, (int) (Math.random() * 100));
				pst.setString(3, "2016-09-0" + (int) (random.nextInt(9) % 9 + 1));

				pst.addBatch();

				if (x % 1000 == 0) {
					pst.executeBatch();
					// pst.executeUpdate(arg0)
				}

				x++;
			}
			System.out.println("Commit");
			pst.executeBatch();
			conn.commit();
			conn.close();
			pst.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}

	}

}