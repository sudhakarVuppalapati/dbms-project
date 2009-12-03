package firstmilestone;

import operator.Operator;
import exceptions.NoSuchColumnException;
import systeminterface.Row;
import systeminterface.Table;

public class MyHelper {
	public static void printArr(Object[] arr){
		StringBuffer sb=new StringBuffer();
		for(Object o: arr){
			sb.append(o.toString()+"\t");
		}
		System.out.println(sb.toString());
	}
	
	public static void printRow(Row r){
		StringBuffer sb=new StringBuffer();
		try{
			for(String colName:r.getColumnNames())
				sb.append(r.getColumnValue(colName)+"\t");
			System.out.println(sb.toString());
		}
		catch(NoSuchColumnException nsce){
			System.out.println("no such column");
		}
	}
	
	public  static void printTable(Table t){
		try{
		System.out.println(t.getTableName());
		
		Operator<Row> op=(Operator<Row>)t.getRows();
		op.open();
		Row first=op.next();
		StringBuffer sb=new StringBuffer();
		
		for(String colName:first.getColumnNames())
			sb.append(colName+"  \t");
		
		System.out.println(sb.toString());
		
		sb=new StringBuffer();
		for(String colName:first.getColumnNames())
			sb.append(first.getColumnValue(colName)+"\t");
		
		System.out.println(sb.toString());
		
		Row r;
		int noRows=0;
		while((r=op.next())!=null){
			sb=new StringBuffer();
			for(String colName:r.getColumnNames())
				sb.append(r.getColumnValue(colName)+"\t");
			System.out.println(sb.toString());
			noRows++;
		}
		System.out.println("No of rows: "+(noRows+1));
		
		}
		catch(NoSuchColumnException nsce){
			System.out.println("No such Column in print table");
		}
		
	}
}
