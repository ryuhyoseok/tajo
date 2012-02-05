package nta.engine.ipc.protocolrecords;

import java.net.URI;
import java.util.List;

import nta.common.ProtoObject;
import nta.distexec.DistPlan;
import nta.engine.ExecutorRunnerProtos.ExecutorRunRequestProto;

public interface ExecutorRunRequest extends ProtoObject<ExecutorRunRequestProto>{

	public int getId();
	public DistPlan getDistPlan();
	public List<Fragment> getFragments();
	public boolean isClusteredOutput();
	public URI getOutputDest();
}
