/**
 * 
 */
package tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import tajo.engine.json.GsonCreator;


/**
 * @author Hyunsik Choi
 *
 */
public abstract class UnaryNode extends LogicalNode implements Cloneable {
	@Expose
	LogicalNode subExpr;
	
	public UnaryNode() {
		super();
	}
	
	/**
	 * @param type
	 */
	public UnaryNode(ExprType type) {
		super(type);
	}
	
	public void setSubNode(LogicalNode subNode) {
		this.subExpr = subNode;
	}
	
	public LogicalNode getSubNode() {
		return this.subExpr;
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  UnaryNode unary = (UnaryNode) super.clone();
	  unary.subExpr = (LogicalNode) (subExpr == null ? null : subExpr.clone());
	  
	  return unary;
	}
	
	public void preOrder(LogicalNodeVisitor visitor) {
	  visitor.visit(this);
	  subExpr.preOrder(visitor);
  }
	
	public void postOrder(LogicalNodeVisitor visitor) {
	  subExpr.postOrder(visitor);	  
	  visitor.visit(this);
	}

  public String toJSON() {
    subExpr.toJSON();
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
