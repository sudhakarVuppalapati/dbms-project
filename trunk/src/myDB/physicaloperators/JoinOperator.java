package myDB.physicaloperators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import exceptions.NoSuchColumnException;

import metadata.Type;
import metadata.Types;
import myDB.MyColumn;
import myDB.MyNull;

import systeminterface.Column;

/**
 * 
 * @author razvan
 * Implementation of the JOIN relational operator
 * by different strategies/methods (each strategy/method  corresponds
 * to a java method in this class)
 */
public class JoinOperator {
	
	
	/**
	 * <b>Simple Nested loops</b> implementation:the basic join
	 * implementation. Even if is not the smartest join implementation
	 * it might be the only solution (Block nested loops doesn't 
	 * make sense since we're not dealing with I/O) 
	 */
	
	public Map<String,Column> joinSimple(Map<String,Column> input1,
										 Map<String,Column> input2,
										 String leftAttr,String rightAttr) 
										 throws NoSuchColumnException{
		Column leftCol,rightCol;
		Type colType;
		
		leftCol=input1.get(leftAttr);
		rightCol=input2.get(rightAttr);
		
		
		
		//set of working array vars
		int[] intArrLeft=null,intArrRight=null;
		long[] longArrLeft=null,longArrRight=null;
		float[] floatArrLeft=null,floatArrRight=null;
		double[] doubleArrLeft=null,doubleArrRight=null;
		Object[] dateArrLeft=null,dateArrRight=null;
		Object[] stringArrLeft=null,stringArrRight=null;
		
		//aux column
		Column c;
		
		//aux 
		int i,j,k=0,l=0;
		String str1,str2;
		
		//aux arrays to maintain the rowIds from the two tables
		int crossProdSize=leftCol.getRowCount() * rightCol.getRowCount();
		int leftRows[]=new int[crossProdSize]; // the size is bigger than needed
		int rightRows[]=new int[crossProdSize];// but this is the only way
		
		//initialize the two arrays to -1
		/*for(i=0;i<crossProdSize;i++){
			leftRows[i]=rightRows[i]=-1;
		}*/
		
		if(leftCol==null || rightCol==null){ // the input relation doesn't contain prjAttributes[i] column
				throw new NoSuchColumnException();
		}
		
		colType=leftCol.getColumnType();
		
		if(colType==Types.getIntegerType()){
			intArrLeft=(int[])(leftCol.getDataArrayAsObject());
			intArrRight=(int[])(rightCol.getDataArrayAsObject());
			for(i=0;i<intArrLeft.length;i++)
				for(j=0;j<intArrRight.length;j++){
					if(intArrLeft[i]!=Integer.MAX_VALUE &&
							intArrRight[j]!=Integer.MAX_VALUE &&
							intArrLeft[i]==intArrRight[j]){ 
						leftRows[k]=i;
						rightRows[k++]=j;
					}
				}
		}
		else if(colType== Types.getLongType()){
			longArrLeft=(long[])(leftCol.getDataArrayAsObject());
			longArrRight=(long[])(rightCol.getDataArrayAsObject());
			for(i=0;i<longArrLeft.length;i++)
				for(j=0;j<longArrRight.length;j++){
					if(longArrLeft[i]!=Long.MAX_VALUE &&
							longArrRight[j]!=Long.MAX_VALUE &&
							longArrLeft[i]==longArrRight[j]){ 
						leftRows[k]=i;
						rightRows[k++]=j;
					}
				}
		}
		else if(colType== Types.getFloatType()){
			floatArrLeft=(float[])(leftCol.getDataArrayAsObject());
			floatArrRight=(float[])(rightCol.getDataArrayAsObject());
			for(i=0;i<floatArrLeft.length;i++)
				for(j=0;j<floatArrRight.length;j++){
					if(floatArrLeft[i]!=Float.MAX_VALUE &&
							floatArrRight[j]!=Float.MAX_VALUE &&
							floatArrLeft[i]==floatArrRight[j]){ 
						leftRows[k]=i;
						rightRows[k++]=j;
					}
				}
		}
		else if(colType== Types.getDoubleType()){
			doubleArrLeft=(double[])(leftCol.getDataArrayAsObject());
			doubleArrRight=(double[])(rightCol.getDataArrayAsObject());
			for(i=0;i<doubleArrLeft.length;i++)
				for(j=0;j<doubleArrRight.length;j++){
					if(doubleArrLeft[i]!=Double.MAX_VALUE &&
							doubleArrRight[j]!=Double.MAX_VALUE &&
							doubleArrLeft[i]==doubleArrRight[j]){ 
						leftRows[k]=i;
						rightRows[k++]=j;
					}
				}
		}
		else if(colType== Types.getDateType()){
			dateArrLeft=(Object[])(leftCol.getDataArrayAsObject());
			dateArrRight=(Object[])(rightCol.getDataArrayAsObject());
			for(i=0;i<dateArrLeft.length;i++)
				for(j=0;j<dateArrRight.length;j++){
					if(dateArrLeft[i]!=MyNull.NULLOBJ &&
							dateArrRight[j]!=MyNull.NULLOBJ &&
							((Date)dateArrLeft[i]).equals((Date)dateArrRight[j])){ 
						leftRows[k]=i;
						rightRows[k++]=j;
					}
				}
		}
		else{ // for the cases of Varchar and Char
			stringArrLeft=(Object[])(leftCol.getDataArrayAsObject());
			stringArrRight=(Object[])(rightCol.getDataArrayAsObject());
			for(i=0;i<stringArrLeft.length;i++)
				for(j=0;j<stringArrRight.length;j++){
					str1=(String)stringArrLeft[i];
					str2=(String)stringArrRight[j];
					if(stringArrLeft[i]!=MyNull.NULLOBJ &&
							stringArrRight[j]!=MyNull.NULLOBJ &&
							((String)stringArrLeft[i]).equals((String)stringArrRight[j])){ 
						leftRows[k]=i;
						rightRows[k++]=j;
					}
				}
		}
		
		//now create the resulting arrays(drop what doesn't qualify for the
		//join and keep(probably multiple times) what qualifies - 
		//!!! this might create some problems :too many arrays to be recreated
		
		//take the first map of columns,recreate the new arrays and set them as data of the column
		
		Iterator<String> colIter=input1.keySet().iterator();
		
		while(colIter.hasNext()){ // for each column of the first input
			c=input1.get(colIter.next());
			colType=c.getColumnType();
			if(colType==Types.getIntegerType()){
				//declare new array to hold the new column data
				int[] newColData=new int[k]; // k is the cardinality of the join
				
				
				
				//copy from the old array of the column
				intArrLeft=(int[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData[i]=intArrLeft[leftRows[i]];
				}
				
				//set the new array as the new data of the column
				((MyColumn)c).setData(newColData,k);
			}
			else if(colType==Types.getLongType()){
				long[] newColData=new long[k]; // k is the cardinality of the join
				
				longArrLeft=(long[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData[i]=longArrLeft[leftRows[i]];
				}
				((MyColumn)c).setData(newColData,k);
			}
			else if(colType==Types.getFloatType()){
				float[] newColData=new float[k]; // k is the cardinality of the join
				
				floatArrLeft=(float[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData[i]=floatArrLeft[leftRows[i]];
				}
				((MyColumn)c).setData(newColData,k);
			}
			else if(colType==Types.getDoubleType()){
				double[] newColData=new double[k]; // k is the cardinality of the join
				
				doubleArrLeft=(double[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData[i]=floatArrLeft[leftRows[i]];
				}
				((MyColumn)c).setData(newColData,k);
			}
			else { // if Object type
				List newColData=new ArrayList(k); // k is the cardinality of the join
				
				dateArrLeft=(Object[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData.add(i,dateArrLeft[leftRows[i]]);
				}
				((MyColumn)c).setData(newColData,k);
			}
			
		}
		
		
		//do the same thing for the second input hashmap
		//besides that add the columns of this hashmap to the first input hashmap
		
		colIter=input2.keySet().iterator();
		
		while(colIter.hasNext()){ // for each column of the first input
			c=input2.get(colIter.next());
			colType=c.getColumnType();
			if(colType==Types.getIntegerType()){
				//declare new array to hold the new column data
				int[] newColData=new int[k]; // k is the cardinality of the join
				
				
				
				//copy from the old array of the column
				intArrLeft=(int[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData[i]=intArrLeft[leftRows[i]];
				}
				
				//set the new array as the new data of the column
				((MyColumn)c).setData(newColData,k);
				input1.put(c.getColumnName(),c);
			}
			else if(colType==Types.getLongType()){
				long[] newColData=new long[k]; // k is the cardinality of the join
				
				longArrLeft=(long[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData[i]=longArrLeft[leftRows[i]];
				}
				((MyColumn)c).setData(newColData,k);
				input1.put(c.getColumnName(),c);
			}
			else if(colType==Types.getFloatType()){
				float[] newColData=new float[k]; // k is the cardinality of the join
				
				floatArrLeft=(float[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData[i]=floatArrLeft[leftRows[i]];
				}
				((MyColumn)c).setData(newColData,k);
				input1.put(c.getColumnName(),c);
			}
			else if(colType==Types.getDoubleType()){
				double[] newColData=new double[k]; // k is the cardinality of the join
				
				doubleArrLeft=(double[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData[i]=floatArrLeft[leftRows[i]];
				}
				((MyColumn)c).setData(newColData,k);
				input1.put(c.getColumnName(),c);
			}
			else { // if Object type
				List newColData=new ArrayList(k); // k is the cardinality of the join
				
				dateArrLeft=(Object[])c.getDataArrayAsObject();
				for(i=0;i<k;i++){
					newColData.add(i,dateArrLeft[leftRows[i]]);
				}
				((MyColumn)c).setData(newColData,k);
				input1.put(c.getColumnName(),c);
			}
			
		}
		
		return input1;
	}
}
