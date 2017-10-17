package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class RankNode implements Comparable<RankNode>
,WritableComparable<RankNode>, Writable{
	
	
	public RankNode(){
		
	}
	// Fields nodeid, and rank
	public RankNode(String nodeid, double pagerank){
		this.nodeid = nodeid;
		this.pageRank = pagerank;
	}
	public String nodeid;
	public double pageRank;

	// Compares pageranks
	
	public int compareTo(RankNode o) {
		return Double.compare(o.pageRank, this.pageRank);
	}


	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, nodeid);
		WritableUtils.writeString(out, pageRank + "");
		
	}

	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		nodeid = WritableUtils.readString(in);
		pageRank = Double.parseDouble(WritableUtils.readString(in));
		
	}
}