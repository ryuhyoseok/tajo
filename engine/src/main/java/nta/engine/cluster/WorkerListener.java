/**
 * 
 */
package nta.engine.cluster;

import java.net.InetSocketAddress;

import nta.engine.NConstants;
import nta.engine.QueryUnitId;
import nta.engine.QueryUnitProtos.InProgressStatus;
import nta.engine.QueryUnitProtos.QueryUnitReportProto;
import nta.engine.ipc.MasterInterface;
import nta.engine.ipc.QueryUnitReport;
import nta.engine.query.QueryUnitReportImpl;
import nta.rpc.NettyRpc;
import nta.rpc.ProtoParamRpcServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.mortbay.log.Log;

/**
 * @author jihoon
 *
 */
public class WorkerListener implements Runnable, MasterInterface {
  
//  private Map<QueryUnitId, InProgressStatus> progressMap;
  
  private final ProtoParamRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private volatile boolean stopped = false;
  private QueryManager qm;
  
  public WorkerListener(Configuration conf, QueryManager qm) {
//      Map<QueryUnitId,InProgressStatus> progressMap) {
    String confMasterAddr = conf.get(NConstants.MASTER_ADDRESS,
        NConstants.DEFAULT_MASTER_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterAddr);
    if (initIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initIsa);
    }
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this, MasterInterface.class, initIsa);
    this.qm = qm;
  }
  
  public InetSocketAddress getBindAddress() {
    return this.bindAddr;
  }
  
  public String getAddress() {
    return this.addr;
  }
  
  public boolean isStopped() {
    return this.stopped;
  }
  
  public void start() {
    this.stopped = false;
    this.rpcServer.start();
    this.bindAddr = rpcServer.getBindAddress();
    this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();
  }
  
  public void stop() {
    this.rpcServer.shutdown();
    this.stopped = true;
  }

  /* (non-Javadoc)
   * @see nta.engine.ipc.AsyncMasterInterface#reportQueryUnit(nta.engine.QueryUnitProtos.QueryUnitReportProto)
   */
  @Override
  public void reportQueryUnit(QueryUnitReportProto proto) {
    Log.info("All progresses in qm: " + qm.getAllProgresses());
    QueryUnitReport report = new QueryUnitReportImpl(proto);
    Log.info("Received progress list size: " + report.getProgressList().size());
    for (InProgressStatus status : report.getProgressList()) {
      qm.updateProgress(new QueryUnitId(status.getId()), status);
      Log.info("Received the report :" + status);
    }
  }

  @Override
  public void run() {
    // rpc listen
    try {
      while (!this.stopped) {
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
