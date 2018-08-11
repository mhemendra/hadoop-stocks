package stock;

import java.util.Arrays;

import org.apache.hadoop.io.Text;

public class StockDataParser {

private String stockName;
private String year;
private float openPrice;
private float highestPrice;
private float lowestPrice;
private float closingPrice;
private float quantity;
private float turnover;

public String getStockName() {
	return stockName;
}
public String getYear() {
	return year;
}
public float getOpenPrice() {
	return openPrice;
}
public float getHighestPrice() {
	return highestPrice;
}
public float getLowestPrice() {
	return lowestPrice;
}
public float getClosingPrice() {
	return closingPrice;
}
public float getQuantity() {
	return quantity;
}
public float getTurnover() {
	return turnover;
}

public boolean parse(Text value){
	String line = value.toString();
	String[] columns = line.split(",");
	if (columns.length==9) {
		stockName = columns[0];
		year = columns[1].split("-")[0];
		openPrice =Float.parseFloat(columns[2]);
		highestPrice =Float.parseFloat(columns[3]);
		lowestPrice =Float.parseFloat(columns[4]);
		closingPrice =Float.parseFloat(columns[6]);
		quantity =Float.parseFloat(columns[7]);
		turnover =Float.parseFloat(columns[8]);
		return true;
	}
	else{
		return false;
	}
}


}
