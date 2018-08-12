package stock;

import org.apache.hadoop.io.Text;

public class StockPreParser {

public boolean parse(Text value){
	String line = value.toString();
	String[] columns = line.split(",");
	if (!line.contains("Date") && columns.length==8) {
		return true;
	}
	else{
		return false;
	}
}


}
