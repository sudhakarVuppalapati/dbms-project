package myDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import metadata.Type;

import operator.Operator;

import exceptions.InvalidPredicateException;
import exceptions.IsLeafException;
import exceptions.NoSuchColumnException;
import exceptions.NoSuchTableException;
import exceptions.NotLeafNodeException;
import exceptions.SchemaMismatchException;
import relationalalgebra.CrossProduct;
import relationalalgebra.Input;
import relationalalgebra.Join;
import relationalalgebra.Projection;
import relationalalgebra.RelationalAlgebraExpression;
import relationalalgebra.Selection;
import sampleDB.LeafPredicateTreeNode;
import systeminterface.IndexLayer;
import systeminterface.PredicateTreeNode;
import systeminterface.StorageLayer;
import util.ComparisonOperator;
import util.LogicalOperator;
import util.RelationalOperatorType;

/**
 * 
 * @author attran
 *
 */
public class Optimizer {

	private StorageLayer storageLayer;
	private HashMap< String , Integer> colPaths;

	public Optimizer(StorageLayer sl){
		this.storageLayer=sl;
		colPaths=new HashMap<String, Integer>();
	}


	public RelationalAlgebraExpression optimize(RelationalAlgebraExpression inputTree)
	throws InvalidPredicateException, NoSuchColumnException {

		getPaths(inputTree, 1);
		return pushSelection(pushProjection(inputTree, 1), 1);
	}


	private RelationalAlgebraExpression pushProjection(RelationalAlgebraExpression inputTree, int level)
	throws NoSuchColumnException, InvalidPredicateException {
		RelationalOperatorType type = inputTree.getType(), subType;
		RelationalAlgebraExpression left, right, child, subLeft, subRight;
		int branch;
		if (type == RelationalOperatorType.INPUT)
			return inputTree;
		else if (type == RelationalOperatorType.SELECTION) {
			return new Selection(pushProjection(((Selection)inputTree).getInput(), level),((Selection)inputTree).getPredicate());
		}
		else if (type == RelationalOperatorType.JOIN) {
			left = ((Join)inputTree).getLeftInput();
			right = ((Join)inputTree).getRightInput();
			return new Join(pushProjection(left, level + 1), pushProjection(right, level + 1), 
					((Join)inputTree).getLeftJoinAttribute(), ((Join)inputTree).getRightJoinAttribute());	
		}
		else if (type == RelationalOperatorType.CROSS_PRODUCT) {
			left = ((CrossProduct)inputTree).getLeftInput();
			right = ((CrossProduct)inputTree).getRightInput();

			return new CrossProduct(pushProjection(left, level + 1), pushProjection(right, level + 1));
		}
		else { // if (type == RelationalOperatorType.PROJECTION) {
			child = ((Projection) inputTree).getInput();
			String[] attrs = ((Projection) inputTree).getProjectionAttributes();
			subType = child.getType();
			if (subType == RelationalOperatorType.INPUT) {
				return inputTree;
			}
			else if (subType == RelationalOperatorType.SELECTION) {

				//check if it's possible to move the projection in front of this selection
				if (checkPushProjectionSelection(((Selection)child).getPredicate(),attrs ))
					return new Selection(pushProjection(new Projection(((Selection)child).getInput(), attrs), level),((Selection)child).getPredicate());
			}
			else if (subType == RelationalOperatorType.PROJECTION) {
				String[] subAttrs = ((Projection) child).getProjectionAttributes();
				if (containAllAttributes(subAttrs, attrs)) {
					return new Projection(((Projection)child).getInput(), attrs);
				}
				else throw new NoSuchColumnException(); 
			}
			else if (subType == RelationalOperatorType.JOIN) {
				subLeft = ((Join)child).getLeftInput();
				subRight = ((Join)child).getRightInput();

				List<String> leftAttrs = new ArrayList<String>();
				List<String> rightAttrs = new ArrayList<String>();
				String leftJoinAttr = ((Join) child).getLeftJoinAttribute();
				String rightJoinAttr = ((Join) child).getRightJoinAttribute();
				boolean turnLeft = containAllAttributes(attrs, new String[]{leftJoinAttr});
				boolean turnRight = containAllAttributes(attrs, new String[]{rightJoinAttr});
				for (String attr : attrs) {
					if (colPaths.containsKey(attr)) {
						branch = (int) (colPaths.get(attr).intValue() % Math.pow(10, level));
						if (branch == 1 && turnLeft) {//go left
							leftAttrs.add(attr);
						}
						else if (branch == 2 && turnRight) { //go right
							rightAttrs.add(attr);
						}
						else {
							throw new InvalidPredicateException();
						} 
					}
					else throw new NoSuchColumnException();
				}
				if (leftAttrs.size() > 0)
					if (rightAttrs.size() > 0)
						return new Join(pushProjection(new Projection(subLeft, leftAttrs.toArray(new String[0])), level + 1), 
								pushProjection(new Projection(subRight, rightAttrs.toArray(new String[0])), level + 1),
								leftJoinAttr, rightJoinAttr);
					else
						return new Projection(new Join(pushProjection(new Projection(subLeft, leftAttrs.toArray(new String[0])), level + 1), 
								pushProjection(subRight, level + 1),
								leftJoinAttr, rightJoinAttr), attrs);
				else if (rightAttrs.size() > 0)
					return new Projection(new Join(pushProjection(subLeft, level + 1), 
							pushProjection(new Projection(subRight, rightAttrs.toArray(new String[0])), level + 1),
							leftJoinAttr, rightJoinAttr), attrs);
				else //Dont push projection
					return new Projection(new Join(pushProjection(subLeft, level + 1), pushProjection(subRight, level + 1), leftJoinAttr, rightJoinAttr), attrs);
			}
			else if (subType == RelationalOperatorType.CROSS_PRODUCT) {
				subLeft = ((CrossProduct)child).getLeftInput();
				subRight = ((CrossProduct)child).getRightInput();
				List<String> leftAttrs = new ArrayList<String>();
				List<String> rightAttrs = new ArrayList<String>();
				for (String attr : attrs) {
					if (colPaths.containsKey(attr)) {
						branch = (int) (colPaths.get(attr).intValue() % Math.pow(10, level));
						if (branch == 1) {//go left
							leftAttrs.add(attr);
						}
						else if (branch == 2) { //go right
							rightAttrs.add(attr);
						}
						else {
							throw new InvalidPredicateException();
						} 
					}
					else throw new NoSuchColumnException();
				}
				if (leftAttrs.size() > 0)
					if (rightAttrs.size() > 0)
						return new CrossProduct(pushProjection(new Projection(subLeft, leftAttrs.toArray(new String[0])), level + 1), 
								pushProjection(new Projection(subRight, rightAttrs.toArray(new String[0])), level + 1));
					else
						return new Projection(new CrossProduct(pushProjection(new Projection(subLeft, leftAttrs.toArray(new String[0])), level + 1), 
								pushProjection(subRight, level + 1)), attrs);
				else if (rightAttrs.size() > 0)
					return new Projection(new CrossProduct(pushProjection(subLeft, level + 1), 
							pushProjection(new Projection(subRight, rightAttrs.toArray(new String[0])), level + 1)), attrs);
				else //Dont push projection
					return new Projection(new CrossProduct(pushProjection(subLeft, level + 1), pushProjection(subRight, level + 1)), attrs);
			}
			else throw new InvalidPredicateException();

		}
		return inputTree;
	}

	private RelationalAlgebraExpression pushSelection(RelationalAlgebraExpression inputTree, int level) 
	throws InvalidPredicateException, NoSuchColumnException {		
		//Temporary variables
		RelationalAlgebraExpression left, right, child, subLeft, subRight;
		RelationalOperatorType type = inputTree.getType(), subType;
		PredicateTreeNode predicate;
		int branch;
		if (type == RelationalOperatorType.JOIN) {
			left = ((Join)inputTree).getLeftInput();
			right = ((Join)inputTree).getRightInput();

			return new Join(pushSelection(left, level + 1), pushSelection(right, level + 1), 
					((Join)inputTree).getLeftJoinAttribute(), ((Join)inputTree).getRightJoinAttribute());
		}
		else if (type == RelationalOperatorType.CROSS_PRODUCT) {
			left = ((CrossProduct)inputTree).getLeftInput();
			right = ((CrossProduct)inputTree).getRightInput();

			return new CrossProduct(pushSelection(left, level + 1), pushSelection(right, level + 1));
		}
		else if (type == RelationalOperatorType.PROJECTION || type == RelationalOperatorType.INPUT) {
			return inputTree;
		}
		/**
		 * If current node is selection, we apply rule 3 : Push selection through Joins if:
		 * - The selection predicate is of conjunctive form
		 * - The selection predicate doesn't contain any strange attributes
		 */
		else {
			/** Firstly, we check that if the current treeNode is dummy node, then we push down, ignoring current node */
			predicate = ((Selection)inputTree).getPredicate();	
			
			child = ((Selection)inputTree).getInput();
			subType = child.getType();
			if (subType == RelationalOperatorType.PROJECTION || subType == RelationalOperatorType.INPUT)
				return inputTree;
			else if (subType == RelationalOperatorType.SELECTION) {
				return new Selection(((Selection)child).getInput(), 
						new MyPredicateTreeNode(((Selection)child).getPredicate(),LogicalOperator.AND, predicate));
			}
			//Push Selection through the Join
			else if (subType == RelationalOperatorType.JOIN) {
				subLeft = ((Join)child).getLeftInput();
				subRight = ((Join)child).getRightInput();
				try {	
					if (predicate.isLeaf()) {
						if (colPaths.containsKey(predicate.getColumnName())) {
							branch = (int) (colPaths.get(predicate.getColumnName()).intValue() % Math.pow(10, level));
							if (branch == 1) {//go left
								return new Join(pushSelection(new Selection(subLeft, predicate), level + 1),
										pushSelection(subRight, level + 1),
										((Join)child).getLeftJoinAttribute(),
										((Join)child).getRightJoinAttribute());
							}
							else if (branch == 2) { //go right
								return new Join(pushSelection(subLeft, level + 1),
										pushSelection(new Selection(subRight, predicate), level + 1),
										((Join)child).getLeftJoinAttribute(),
										((Join)child).getRightJoinAttribute());
							}
							else {
								throw new InvalidPredicateException();
							}
						}
						else throw new NoSuchColumnException();
					}

					else
						//Not a conjunctive form, do not pass through join
						return new Selection(new Join(pushSelection(subLeft, level + 1), 
								pushSelection(subRight, level + 1),
								((Join)child).getLeftJoinAttribute(),
								((Join)child).getRightJoinAttribute()), predicate);
					
				} catch (NotLeafNodeException e) {
					e.printStackTrace();
				}
			}
			//Push Selection through the Cross Product
			else {
				predicate = ((Selection)inputTree).getPredicate();
				subLeft = ((CrossProduct)child).getLeftInput();
				subRight = ((CrossProduct)child).getRightInput();
				try {
					if (predicate.isLeaf()) {
						if (colPaths.containsKey(predicate.getColumnName())) {
							branch = (int) (colPaths.get(predicate.getColumnName()).intValue() % Math.pow(10, level));
							if (branch == 1) {//go left
								return new CrossProduct(pushSelection(new Selection(subLeft, predicate), level + 1),
										pushSelection(subRight, level + 1));
							}
							else if (branch == 2) { //go right
								return new CrossProduct(pushSelection(subLeft, level + 1),
										pushSelection(new Selection(subRight, predicate), level + 1));
							}
							else throw new InvalidPredicateException();
						}
						else throw new NoSuchColumnException();
					}

					else
						//Not a conjunctive form, do not pass through join
						return new Selection(new CrossProduct(pushSelection(subLeft, level + 1), 
								pushSelection(subRight, level + 1)), predicate);
					
				} catch (NotLeafNodeException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	public void getPaths(RelationalAlgebraExpression tree, int direction ){

		RelationalOperatorType type= tree.getType();

		if(type == RelationalOperatorType.JOIN){
			getPaths( ((Join)tree).getLeftInput(), transform(1, direction));
			getPaths( ((Join)tree).getRightInput(), transform(2, direction));
		}
		else if(type== RelationalOperatorType.CROSS_PRODUCT){
			getPaths(((CrossProduct)tree).getLeftInput(), transform(1, direction));
			getPaths( ((CrossProduct)tree).getRightInput(), transform(2, direction));
		}
		else if(type== RelationalOperatorType.INPUT){

			((Input)tree).getRelationName();
			Map<String, Type> colNames=null;
			try {
				colNames = ((MyTable)(storageLayer.getTableByName(((Input)tree).getRelationName()))).getTableSchema();
			} catch (NoSuchTableException e) {
				e.printStackTrace();
			}

			for(String colName: colNames.keySet())
				colPaths.put(colName, transform(1, direction));
		}
		else if( type== RelationalOperatorType.PROJECTION){

			/*String attributes[]= ((Projection)tree).getProjectionAttributes();
			for(String attr: attributes){
				colPaths.put(attr, direction);
			}*/
			getPaths(((Projection)tree).getInput(),direction);
		}
		else {
			getPaths(((Selection)tree).getInput(),direction);
		}

	}

	/*
	 * iterate through the predicate tree and see if it contains 
	 * all the attributes in the prjAttrs array
	 */
	private static boolean checkPushProjectionSelection(PredicateTreeNode predicate, String[] prjAttrs){

		/*
		 *  searching the predicate tree for each of the prjAttrs is worse than
		 *  traversing the predicate tree in order to gather all the attribute names and 
		 *  than to compare these attributes with the ones in the prjAttrs
		 */

		//traverse the entire predicate tree to get all the existing attributes and put them in a
		//HMap
		List<String> attrs = new ArrayList<String>();
		getAllAttributesInPredicateTree(predicate, attrs);

		boolean found;

		for (String selAttr : attrs) {
			found = false;
			for (String prjAttr : prjAttrs) {
				if (prjAttr.equalsIgnoreCase(selAttr)) {
					found = true; break;
				}
			}
			if (!found) return false;
		}
		return true;
	}

	private static boolean containAllAttributes(String[] bigAttrs, String[] smallAttrs) {
		boolean found = false;
		for (String attr : smallAttrs) {
			found = false;
			for (String bigAttr : bigAttrs) {
				if (bigAttr.equalsIgnoreCase(attr)) {
					found = true;
					break;
				}
			}
			if (!found) return false;
		}
		return true;
	}

	private static void getAllAttributesInPredicateTree(PredicateTreeNode root, List<String> attrs){

		try {
			
			if (root.isLeaf()) attrs.add(root.getColumnName());
			else {
				getAllAttributesInPredicateTree(root.getLeftChild(), attrs);
				getAllAttributesInPredicateTree(root.getRightChild(), attrs);
			}
		} catch (NotLeafNodeException e) {
			e.printStackTrace();
		} catch (IsLeafException e) {
			e.printStackTrace();
		}

	}

	public static void main(String args[]){

		/*r=new Input("R");
		s=new Input("S");
		pr=new Projection(r,new  String[]{"r.r1","r.r2"});

		PredicateTreeNode ptn= new LeafPredicateTreeNode("r.r2",
				ComparisonOperator.GT, new Integer(8));

		sel=new Selection(s,ptn);
		j=new Join(pr,sel,"r.r1","s.s1");

		printRAE(j);*/
		System.out.println(transform(12, 63443));
		System.out.println(12343 % 1000);
		System.out.println(containAllAttributes(new String[]{"hello", " ", "World"}, new String[]{"Hello", "world"}));

	}

	private static int transform(int b, int a) {
		int i = 0;
		if (a == 0) return b;
		while ((double)(a / (Math.pow(10, ++i))) >= 1.0d);
		return (int)(b * Math.pow(10, i) + a);
	}

	/*private static void getConjuncts(PredicateTreeNode predicate, List<PredicateTreeNode> conjuncts) 
	throws SchemaMismatchException {

		

		if (predicate.isLeaf()) conjuncts.add(predicate);
		else
			try {
				if (predicate.getLogicalOperator() == LogicalOperator.OR) throw new SchemaMismatchException();
				else {
					getConjuncts(predicate.getLeftChild(), conjuncts);
					getConjuncts(predicate.getRightChild(), conjuncts);
				}
			} catch (IsLeafException e) {
				e.printStackTrace();
			} 
	}*/
}
