package tajo.master;

import java.io.IOException;

public interface EngineService {
	public void init() throws IOException;
	public void shutdown() throws IOException;
}
