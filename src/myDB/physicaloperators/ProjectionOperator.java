package myDB.physicaloperators;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import metadata.Type;
import metadata.Types;

import exceptions.NoSuchColumnException;
import systeminterface.Column;


/**
 * 
 * @author razvan
 * 
 * implementation of the Projection relational operator
 * by different strategies/methods (each strategy/method  corresponds
 * to a java method in the class)
 */
public class ProjectionOperator {
	
	/**
	 * 
	 * @param input - The relation passed/pipelined as input(from previous operators)
	 * @param prjAtrributes - The projection attributes
	 * @return - the result of projecting all the tuples of the input(relation), after duplicate
	 * elimination
	 * 
	 * This method implements the most basic strategy of projection, i.e.: scanning,sorting,eliminating duplicates
	 */
	public Map<String,Column> project(Map<String,Column> input, String[] prjAttributes)
	throws NoSuchColumnException{
		
		//variable to store the final result of the projection
		Map<String,Column> result=new HashMap<String,Column>();
		
		Column c;// aux, working var
		Type colType;
		
		Map<Column,Object> tmpArrContainer=new HashMap<Column,Object>(); //aux var for holding the sorted 
																		 //arrays so that I don't have to call 
																		 //getDataArrayAsObject repeatedly
		
		//set of working array vars
		int[] intArr;
		long[] longArr;
		float[] floatArr;
		double[] doubleArr;
		Date[] dateArr;
		String[] stringArr;
		
		/*iterate through the set of columns and put into the result only the columns
		 *whose names appear in the list of projection attributes (and also sort the column)
		 */
		for(int i=0;i<prjAttributes.length;i++){
			c= input.get(prjAttributes[i]);
			if(c==null){ // the input relation doesn't contain prjAttributes[i] column
				throw new NoSuchColumnException();
			}
			//else
			colType=c.getColumnType();
			
			if(c==Types.getIntegerType()){
				intArr=(int[])(c.getDataArrayAsObject());
				Arrays.sort(intArr);
				tmpArrContainer.put(c,intArr);
			}
			else if(c== Types.getLongType()){
				longArr=(long[])(c.getDataArrayAsObject());
				Arrays.sort(longArr);
				tmpArrContainer.put(c,longArr);
			}
			else if(c== Types.getFloatType()){
				floatArr=(float[])(c.getDataArrayAsObject());
				Arrays.sort(floatArr);
				tmpArrContainer.put(c,floatArr);
			}
			else if(c== Types.getDoubleType()){
				doubleArr=(double[])(c.getDataArrayAsObject());
				Arrays.sort(doubleArr);
				tmpArrContainer.put(c,doubleArr);
			}
			else if(c== Types.getDateType()){
				dateArr=(Date[])(c.getDataArrayAsObject());
				Arrays.sort(dateArr);
				tmpArrContainer.put(c,dateArr);
			}
			else{ // for the case of Varchar and Char
				stringArr=(String[])(c.getDataArrayAsObject());
				Arrays.sort(stringArr);
				tmpArrContainer.put(c,stringArr);
			}
		
	    }
		
		//By this point the all the columns are sorted independently which is wrong!!!!
		
		return null;
	}
	
	
	
	/**
	 * This implementation doesn't eliminate duplicates ... which might also be a kind of projection
	 */
	public Map<String,Column> projectWithDuplicates(Map<String,Column> input, String[] prjAttributes)
	throws NoSuchColumnException{
		
		Map<String,Column> result=new HashMap<String,Column>();
		
		Column c;// aux, working var
		
		
		
		
		//verify if the projection is possible,i.e.: all the projection attributes are in the input table
		for(int i=0;i<prjAttributes.length;i++){
			c= input.get(prjAttributes[i]);
			if(c==null){ // the input relation doesn't contain prjAttributes[i] column
				throw new NoSuchColumnException();
			}
			//else
			result.put(prjAttributes[i],c);
		}
	
		return result;
	}
}
