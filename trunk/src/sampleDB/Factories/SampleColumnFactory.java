package sampleDB.Factories;

import metadata.Type;
import sampleDB.SampleColumn;
import systeminterface.Column;
import util.ArgumentFactory;

/**
 * @author myahya
 * 
 */
public class SampleColumnFactory implements ArgumentFactory<Column, Type> {

	@Override
	public Column newInstance(Type type) {
		// TODO Auto-generated method stub

		Column col = new SampleColumn();

		if (type.getName().compareTo("Char") == 0) {

		}

		else if (type.getName().compareTo("Date") == 0) {

		}

		else if (type.getName().compareTo("Double") == 0) {

		}

		else if (type.getName().compareTo("Float") == 0) {

		}

		else if (type.getName().compareTo("metadata.Integer") == 0) {

			col.setColumnType(type);
		}

		else if (type.getName().compareTo("Long") == 0) {

		}

		else if (type.getName().compareTo("metadata.Varchar") == 0) {

			col.setColumnType(type);
		}

		else
			return null;

		return col;
	}

}
