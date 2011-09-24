package com.jayway.hadoop.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/**
 * <p>Simple utility for parsing ikea log4j file</p>
 * 
 * 2011-05-19 20:29:27,340 [icard-EBCCARDPAY1-BsVerifyLoadCard-2011-05-19T20:29:27Z-FE6E04706F8FAB9DCA55223531B2C10D-2] WARN  
 *    com.ikea.framework.business.tasks.util.BtTxRunnerBean  - Rollback for com.ikea.ebccardpay1.cardpayment.bt.implementation.BtVerifyLoadCardImpl
 * 
 * @author johanrask
 *
 */
public class IkeaLogUtils {

	public static DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,S");

	static final Pattern pattern = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2},\\d{3})\\s(\\[.*\\])\\s(DEBUG|INFO|WARN|ERROR|FATAL)\\s{1,2}(.*)\\s{2}-\\s(.*)(%%%)$",
			Pattern.DOTALL);
	
	/**
	 * Returns a {@link MatchResult} that you can use to get each group in the pattern.
	 * 
	 * Matchresult.group(1) => "2011-05-19 20:29:27,340"
	 * Matchresult.group(2) => "[icard-EBCCARDPAY1-BsVerifyLoadCard-2011-05-19T20:29:27Z-FE6E04706F8FAB9DCA55223531B2C10D-2]"
	 * Matchresult.group(3) => "WARN"
	 * etc...
	 * 
	 * @param key
	 * @param row
	 * @return
	 */
	public static MatchResult entry(LongWritable key,Text row) {
		
		String line = row.toString() + "%%%";
		
		if(!pattern.matcher(line).matches()) {
			System.out.println(String.format("%s does not match %s",pattern.toString(),line));
			return null;
		}

		Scanner scanner = new Scanner(line);
		scanner.findInLine(pattern);
		MatchResult result = scanner.match();
		
		return result;
		
	}
}
