package test;

import sampleDB.SampleRow;
import sampleDB.Factories.SampleColumnFactory;
import sampleDB.operators.SampleOperator;
import systeminterface.Column;
import systeminterface.Database;
import systeminterface.Row;
import exceptions.ColumnAlreadyExistsException;
import exceptions.NoSuchTableException;
import exceptions.TableAlreadyExistsException;

/**
 * @author myahya
 * 
 */
public class Sample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SampleColumnFactory colFactory = new SampleColumnFactory();

		Database.initDatabase();

		Database myDatabase = Database.getInstance();

		System.out.println("Starting DB");
		myDatabase.startSystem();

		try {
			myDatabase.getStorageInterface().createTable("StudentGrades");
		} catch (TableAlreadyExistsException e) {
			System.err.println("TableAlreadyExists");
			e.printStackTrace();
		}

		Column nameCol = colFactory.newInstance(new metadata.Varchar());
		if (nameCol != null) {
			nameCol.setColumnName("name");
		} else {

			System.err.println("Failed to get new col");
		}

		Column gradeCol = colFactory.newInstance(new metadata.Integer());
		gradeCol.setColumnName("grade");

		try {
			myDatabase.getStorageInterface().getTableByName("StudentGrades")
					.addColumn(nameCol);
			myDatabase.getStorageInterface().getTableByName("StudentGrades")
					.addColumn(gradeCol);

		} catch (ColumnAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchTableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		SampleOperator<Row> inputOperator = new SampleOperator<Row>();

		/* should be done in a smarter way */
		SampleRow r1 = new SampleRow();
		Object[] r1Contents = { "Mohamed", new Integer(4) };
		r1.initializeRow(r1Contents);
		SampleRow r2 = new SampleRow();
		Object[] r2Contents = { "Alekh", new Integer(1) };
		r2.initializeRow(r2Contents);

		inputOperator.add(r1);
		inputOperator.add(r2);

		/* Take this out! */
		/*
		 * inputOperator.open(); Row r; while((r=inputOperator.next())!=null){
		 * try { System.out.println("Name: "+r.getElement(1));
		 * System.out.println("Grade: "+r.getElement(2)); } catch (Exception e)
		 * { // TODO Auto-generated catch block e.printStackTrace(); } }
		 * inputOperator.close();
		 */

		try {
			myDatabase.getStorageInterface().getTableByName("StudentGrades")
					.addRow(r1);
			myDatabase.getStorageInterface().getTableByName("StudentGrades")
					.addRow(r2);
		} catch (NoSuchTableException e) {
			System.err.println("NoSuchTable");
			e.printStackTrace();
		}

		try {
			System.out.print(myDatabase.getStorageInterface().getTableByName(
					"StudentGrades"));
		} catch (NoSuchTableException e) {
			System.err.println("NoSuchTable");
			e.printStackTrace();
		}

		System.out.println("Shutting down DB");
		myDatabase.shutdownSystem();

	}

}
