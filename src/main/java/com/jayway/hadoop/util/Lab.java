package com.jayway.hadoop.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Lab {

	
	public static void main(String[] args) throws ParseException {
		
		SimpleDateFormat format = new SimpleDateFormat("E");
		
		System.out.println(format.format(new Date()));
		
		
		format = new SimpleDateFormat("yyyyMMdd");
		System.out.println(format.parseObject("20110520"));
	}
}
