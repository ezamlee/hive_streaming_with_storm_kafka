package com.storm.topology;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class HiveOutputBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
		ofDeclarer.declare(new Fields("application_id","term_months","first_name","last_name","address","state","phone","type",
				"origination_date","loan_decision_type","decided","loan_approved","denial_reason","loan_account_status",
				"approved","payment_method","requested_amount","funded","funded_date","rate","ecoagroup","officer_id","denied_date"));
	}

	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		try {
			String loanDataStr = tuple.getString(0);
			String[] loanData = loanDataStr.split("\t");
			Values values = new Values(String.valueOf(loanData[0]),
					String.valueOf(loanData[1]), String.valueOf(loanData[2]),
					String.valueOf(loanData[3]), String.valueOf(loanData[4]),
					String.valueOf(loanData[5]), String.valueOf(loanData[6]),
					String.valueOf(loanData[7]), String.valueOf(loanData[8]),
					String.valueOf(loanData[9]), String.valueOf(loanData[10]),
					String.valueOf(loanData[11]), String.valueOf(loanData[12]),
					String.valueOf(loanData[13]), String.valueOf(loanData[14]),
					String.valueOf(loanData[15]), String.valueOf(loanData[16]),
					String.valueOf(loanData[17]), String.valueOf(loanData[18]),
					String.valueOf(loanData[19]),String.valueOf(loanData[20]),
					String.valueOf(loanData[21]),String.valueOf(loanData[22])
					);
			outputCollector.emit(values);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
}
