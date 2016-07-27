package main.java.perftest.bean;

import java.util.Date;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace="test", name="table")
public class ClientBean {

	@PartitionKey @Column(name="a")
	private Integer a;
	@ClusteringColumn (value=0) @Column(name="b")
	private String b;
	@ClusteringColumn (value=1) @Column(name="c")
	private String c;
	@ClusteringColumn (value=2) @Column(name="d")
	private Date d;
	@ClusteringColumn (value=3) @Column(name="e")
	private Integer e;
	@Column(name="f")
	private Integer f;
	@Column(name="g")
	private String g;
	
	public Integer getA() {
		return a;
	}
	public void setA(Integer a) {
		this.a = a;
	}
	public String getB() {
		return b;
	}
	public void setB(String b) {
		this.b = b;
	}
	public String getC() {
		return c;
	}
	public void setC(String c) {
		this.c = c;
	}
	public Date getD() {
		return d;
	}
	public void setD(Date d) {
		this.d = d;
	}
	public Integer getE() {
		return e;
	}
	public void setE(Integer e) {
		this.e = e;
	}
	public Integer getF() {
		return f;
	}
	public void setF(Integer f) {
		this.f = f;
	}
	public String getG() {
		return g;
	}
	public void setG(String g) {
		this.g = g;
	}
}
