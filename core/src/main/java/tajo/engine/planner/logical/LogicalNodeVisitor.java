/**
 * 
 */
package tajo.engine.planner.logical;


/**
 * @author Hyunsik Choi
 */
public interface LogicalNodeVisitor {  
  void visit(LogicalNode node);
}
