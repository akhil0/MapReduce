package mr;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;



public class Node implements Writable {

	
	private Double pagerank;
	private String[] adjNodes;
	private boolean isPrimaryNode;

	public Node() {
	}

	// Constructor
	public Node(Double pagerank , String[] adjNodes, 
			boolean isPrimaryNode) {
		this.pagerank = pagerank;
		this.adjNodes = adjNodes;
		this.isPrimaryNode = isPrimaryNode;
	}

	
	public String[] getAdjNodeList(){
		return this.adjNodes;
	}
	
	public void setAdjNodeList(String[] adjNodes) {
		this.adjNodes = adjNodes;
	}
	
	public void setPageRank(Double pagerank){ 
		this.pagerank = pagerank;
	}
	

	public Double getPageRank(){
		return this.pagerank;
	}
	
	// Boolean for primary Node identification
	public void setPrimaryNode(boolean isPrimaryNode){
		this.isPrimaryNode = isPrimaryNode;
	}
	
	public boolean getPrimaryNode(){
		return this.isPrimaryNode;
	} 
	
	// Reads the data input into object in the order
	public void readFields(DataInput dataInput) throws IOException {
		pagerank = Double.parseDouble(WritableUtils.readString(dataInput));
		adjNodes = WritableUtils.readStringArray(dataInput);
		isPrimaryNode = Boolean.parseBoolean(WritableUtils.readString(dataInput));
	}

	// Writes the data output into object in order
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, pagerank + "");
		WritableUtils.writeStringArray(dataOutput, adjNodes);
		WritableUtils.writeString(dataOutput, isPrimaryNode + "");
	}
}
