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
public abstract class BinaryNode extends LogicalNode implements Cloneable {
	@Expose
	LogicalNode outer = null;
	@Expose
	LogicalNode inner = null;
	
	public BinaryNode() {
		super();
	}
	
	/**
	 * @param opType
	 */
	public BinaryNode(ExprType opType) {
		super(opType);
	}
	
	public LogicalNode getOuterNode() {
		return this.outer;
	}
	
	public void setOuter(LogicalNode op) {
		this.outer = op;
	}

	public LogicalNode getInnerNode() {
		return this.inner;
	}

	public void setInner(LogicalNode op) {
		this.inner = op;
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  BinaryNode binNode = (BinaryNode) super.clone();
	  binNode.outer = (LogicalNode) outer.clone();
	  binNode.inner = (LogicalNode) inner.clone();
	  
	  return binNode;
	}
	
	public void preOrder(LogicalNodeVisitor visitor) {
	  visitor.visit(this);
	  outer.postOrder(visitor);
    inner.postOrder(visitor);    
  }
	
	public void postOrder(LogicalNodeVisitor visitor) {
    outer.postOrder(visitor);
    inner.postOrder(visitor);
    visitor.visit(this);
  }

  public String toJSON() {
    outer.toJSON();
    inner.toJSON();
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
