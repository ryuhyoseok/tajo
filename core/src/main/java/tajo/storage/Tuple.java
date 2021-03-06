package tajo.storage;

import tajo.datum.*;

import java.net.InetAddress;

/** 
 * 
 * @author jimin
 * @author Hyunsik Choi
 * 
 */

public interface Tuple {
  
	public int size();
	
	public boolean contains(int fieldid);

  public boolean isNull(int fieldid);
	
	public void clear();
	
	public void put(int fieldId, Datum value);

  public void put(int fieldId, Datum [] values);

  public void put(int fieldId, Tuple tuple);
	
	public void put(Datum [] values);
	
	public Datum get(int fieldId);
	
	public void setOffset(long offset);
	
	public long getOffset();

	public BoolDatum getBoolean(int fieldId);
	
	public ByteDatum getByte(int fieldId);

  public CharDatum getChar(int fieldId);
	
	public BytesDatum getBytes(int fieldId);
	
	public ShortDatum getShort(int fieldId);
	
	public IntDatum getInt(int fieldId);
	
	public LongDatum getLong(int fieldId);
	
	public FloatDatum getFloat(int fieldId);
	
	public DoubleDatum getDouble(int fieldId);
	
	public IPv4Datum getIPv4(int fieldId);
	
	public byte [] getIPv4Bytes(int fieldId);
	
	public InetAddress getIPv6(int fieldId);
	
	public byte [] getIPv6Bytes(int fieldId);
	
	public StringDatum getString(int fieldId);
}
