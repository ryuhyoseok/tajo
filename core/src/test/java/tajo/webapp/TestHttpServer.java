/**
 * 
 */
package tajo.webapp;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.webapp.WebAppContext;
import tajo.conf.TajoConf;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
public class TestHttpServer {
  private static HttpServer server;  
  private final static Random rnd = new Random();
  private final static String TEST_CODE;
  
  static {
    TEST_CODE = "TestHttpServlet_" + rnd.nextInt();
  }
  
  @BeforeClass
  public static final void setUp() throws IOException {
    TajoConf conf = new TajoConf();
    server = new HttpServer("TestHttpServer", "localhost", 0, true, null,
        conf, null);
    server.addServlet("servlet", "/servlet", DummyServlet.class);
    
    WebAppContext context = new WebAppContext();    
    context.setDescriptor("src/test/resources/webapps/TestHttpServer/WEB-INF/web.xml");
    context.setResourceBase("src/test/resources/webapps/TestHttpServer/");
    context.setContextPath("/dummy");
    server.addContext(context, true);
    server.start();
  }
  
  @AfterClass
  public static final void tearDown() throws Exception {
    server.stop();
  }

  public static class DummyServlet extends HttpServlet {
    private static final long serialVersionUID = 4366618372901494571L;

    protected void doGet(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().println(TEST_CODE);
      response.getWriter().println("<br />");
      response.getWriter().println(
          "session=" + request.getSession(true).getId());
    }
  }

  @Test
  public final void testDefaultContext() throws IOException {
    int port = server.getPort();
    URL url = new URL("http://localhost:" + port+"/");

    BufferedReader in = new BufferedReader(new InputStreamReader(
        url.openStream()));
    String line = null;
    boolean found = false;
    while ((line = in.readLine()) != null) {
      if (line.equals("TestHttpServlet-120221"))
        found = true;
    }
    assertTrue(found);    
    in.close();
  }
  
  @Test
  public final void testAddContext() throws IOException, InterruptedException {
    int port = server.getPort();
    URL url = new URL("http://localhost:" + port+"/dummy/");

    BufferedReader in = new BufferedReader(new InputStreamReader(
        url.openStream()));
    String line = null;
    boolean found = false;
    while ((line = in.readLine()) != null) {
      if (line.equals("TestHttpServlet-120221"))
        found = true;
    }
    assertTrue(found);
    in.close();
  }

  @Test
  public final void testAddServlet() throws IOException, InterruptedException {
    int port = server.getPort();
    URL url = new URL("http://localhost:" + port+"/servlet");

    BufferedReader in = new BufferedReader(new InputStreamReader(
        url.openStream()));
    String line = null;    
    boolean found = false;
    while ((line = in.readLine()) != null) {
      if (line.equals(TEST_CODE))
        found = true;
    }    
    assertTrue(found);   
    in.close();
  }
}