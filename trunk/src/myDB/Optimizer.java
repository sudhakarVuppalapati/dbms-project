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


public class Optimizer {

	private static Input r,s;
	private static CrossProduct cp;
	private static Join j;
	private static Projection pr;
	private static Selection sel;
	private StorageLayer storageLayer;
	private HashMap< String , Integer> colPaths;

	public Optimizer(StorageLayer sl){
		this.storageLayer=sl;
		colPaths=new HashMap<String, Integer>();
	}


	public RelationalAlgebraExpression optimize(RelationalAlgebraExpression inputTree)
	throws InvalidPredicateException, NoSuchColumnException {

		getPaths(inputTree, 0);
		return pushSelection(pushProjection(inputTree, 1), 1);
	}


	private RelationalAlgebraExpression pushProjection(RelationalAlgebraExpression inputTree, int level){
		/*RelationalOperatorType type = inputTree.getType(), subType;
		int branch;
		if (type == RelationalOperatorType.INPUT)
			return inputTree;
		else if (type == RelationalOperatorType.SELECTION) {
			return new Selection(pushProjection(((Selection)inputTree).getInput(), level),((Selection)inputTree).getPredicate());
		}*/
		//else if (type == RelationalOperatorType.)
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
			child = ((Selection)inputTree).getInput();
			subType = child.getType();
			if (subType == RelationalOperatorType.PROJECTION || subType == RelationalOperatorType.INPUT)
				return inputTree;
			else if (subType == RelationalOperatorType.SELECTION) {
				predicate = ((Selection)inputTree).getPredicate();
				return new Selection(((Selection)child).getInput(), 
						new MyPredicateTreeNode(((Selection)child).getPredicate(),LogicalOperator.AND, predicate));
			}
			//Push Selection through the Join
			else if (subType == RelationalOperatorType.JOIN) {
				predicate = ((Selection)inputTree).getPredicate();
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
								System.out.println("branch = " + branch);
								throw new InvalidPredicateException();
							}
						}
						else throw new NoSuchColumnException();
					}
					
					else if (predicate.getLogicalOperator() == LogicalOperator.OR)
						//Not a conjunctive form, do not pass through join
						return new Selection(new Join(pushSelection(subLeft, level + 1), 
											  pushSelection(subRight, level + 1),
											 ((Join)child).getLeftJoinAttribute(),
											 ((Join)child).getRightJoinAttribute()), predicate);
					else {
						try {
							List<PredicateTreeNode> conjuncts = new ArrayList<PredicateTreeNode>();
							getConjuncts(predicate, conjuncts);
							MyPredicateTreeNode lefPredicate = new MyPredicateTreeNode();
							MyPredicateTreeNode righPredicate= new MyPredicateTreeNode();
							for (PredicateTreeNode conjunct : conjuncts) {
								if (colPaths.containsKey(conjunct.getColumnName())) {
									branch = (int) (colPaths.get(conjunct.getColumnName()).intValue() % Math.pow(10, level));
									if (branch == 1)
										lefPredicate.addConjunct(conjunct, LogicalOperator.AND);
									else if (branch == 2)
										righPredicate.addConjunct(conjunct, LogicalOperator.AND);
									else throw new InvalidPredicateException();
								}
								else throw new InvalidPredicateException();
							}
							return new Join(pushSelection(new Selection(subLeft, lefPredicate), level + 1),
										    pushSelection(new Selection(subRight, righPredicate), level + 1),
										    ((Join)child).getLeftJoinAttribute(),
										    ((Join)child).getRightJoinAttribute());
						}
						catch (SchemaMismatchException e) {
							return new Selection(new Join(pushSelection(subLeft, level + 1), 
									  pushSelection(subRight, level + 1),
									 ((Join)child).getLeftJoinAttribute(),
									 ((Join)child).getRightJoinAttribute()), predicate);
					
						}
					}
				} catch (NotLeafNodeException e) {
					e.printStackTrace();
				} catch (IsLeafException e) {
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
					
					else if (predicate.getLogicalOperator() == LogicalOperator.OR)
						//Not a conjunctive form, do not pass through join
						return new Selection(new CrossProduct(pushSelection(subLeft, level + 1), 
											  pushSelection(subRight, level + 1)), predicate);
					else {
						try {
							List<PredicateTreeNode> conjuncts = new ArrayList<PredicateTreeNode>();
							getConjuncts(predicate, conjuncts);
							MyPredicateTreeNode lefPredicate = new MyPredicateTreeNode();
							MyPredicateTreeNode righPredicate= new MyPredicateTreeNode();
							for (PredicateTreeNode conjunct : conjuncts) {
								if (colPaths.containsKey(conjunct.getColumnName())) {
									branch = (int) (colPaths.get(conjunct.getColumnName()).intValue() % Math.pow(10, level));
									if (branch == 1)
										lefPredicate.addConjunct(conjunct, LogicalOperator.AND);
									else if (branch == 2)
										righPredicate.addConjunct(conjunct, LogicalOperator.AND);
									else throw new InvalidPredicateException();
								}
								else throw new InvalidPredicateException();
							}
							return new CrossProduct(pushSelection(new Selection(subLeft, lefPredicate), level + 1),
										    pushSelection(new Selection(subRight, righPredicate), level + 1));
						}
						catch (SchemaMismatchException e) {
							return new Selection(new CrossProduct(pushSelection(subLeft, level + 1), 
									  pushSelection(subRight, level + 1)), predicate);
					
						}
					}
				} catch (NotLeafNodeException e) {
					e.printStackTrace();
				} catch (IsLeafException e) {
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
			getPaths( ((Join)tree).getRightInput(), transform(2, direction));
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

			String attributes[]= ((Projection)tree).getProjectionAttributes();
			for(String attr: attributes){
				colPaths.put(attr, direction);
			}			
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
		HashMap<String,Object> attrs=new HashMap<String,Object>();
		getAllAttributesInPredicateTree(predicate, attrs);


		for(int i=0; i<prjAttrs.length; i++){
			if(! attrs.containsKey(prjAttrs[i])) return false;
		}

		return true;
	}


	private static void getAllAttributesInPredicateTree(PredicateTreeNode root, HashMap<String,Object> attrs){

		try {
			attrs.put(root.getColumnName(), null);
			if(! root.isLeaf()){
				getAllAttributesInPredicateTree(root.getLeftChild(), attrs);
				getAllAttributesInPredicateTree(root.getRightChild(), attrs);
			}
		} catch (NotLeafNodeException e) {
			e.printStackTrace();
		} catch (IsLeafException e) {
			e.printStackTrace();
		}

	}

	private static boolean searchAttrInPredicateTree(PredicateTreeNode predicate, String attr){
		try {
			if(predicate.getColumnName().equals(attr)) return true;
			if(predicate.isLeaf()) return false;
			else {
				return  searchAttrInPredicateTree(predicate.getLeftChild(), attr) || 
				searchAttrInPredicateTree(predicate.getRightChild(), attr);
			}
		} catch (NotLeafNodeException e) {
			e.printStackTrace();
			return false;
		} catch (IsLeafException e) {
			e.printStackTrace();
			return false;
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
		
		
	}

	private static void printRAE(RelationalAlgebraExpression root){
		if(root.getType() == RelationalOperatorType.JOIN){

			System.out.print("( ");
			printRAE(((Join)root).getLeftInput());
			System.out.print(" )");

			//System.out.println("\u27D7");
			System.out.print(" JOIN ");

			System.out.print("( ");
			printRAE(((Join)root).getRightInput());
			System.out.print(" )");
		}

		else if(root.getType() == RelationalOperatorType.CROSS_PRODUCT){

			System.out.print("( ");
			printRAE(((CrossProduct)root).getLeftInput());
			System.out.print(" )");

			System.out.print("X");

			System.out.print("( ");
			printRAE(((CrossProduct)root).getRightInput());
			System.out.print(" )");
		}

		else if(root.getType() == RelationalOperatorType.PROJECTION){

			/*System.out.println("\u03A0");*/
			System.out.print(" PRJ ");
			System.out.print("( ");
			printRAE(((Projection)root).getInput());
			System.out.print(" )");

		}

		else if(root.getType() == RelationalOperatorType.SELECTION){

			/*System.out.println("\u03C3");*/
			System.out.print(" SELECT ");
			System.out.print("( ");
			printRAE(((Selection)root).getInput());
			System.out.print(" )");

		}

		else{
			System.out.print(((Input)root).getRelationName());
		}
	}

	private static int transform(int b, int a) {
		int i = 0;
		if (a == 0) return b;
		while ((double)(a / (Math.pow(10, ++i))) >= 1.0d);
		return (int)(b * Math.pow(10, i) + a);
	}
	
	private static void getConjuncts(PredicateTreeNode predicate, List<PredicateTreeNode> conjuncts) 
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
	}
}
