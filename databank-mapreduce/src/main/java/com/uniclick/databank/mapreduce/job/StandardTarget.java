package com.uniclick.databank.mapreduce.job;

/**
 * @ClassName: StandardTarget
 * @Description: TODO(分析中用到的标准指标)
 * @author: ping.jie
 * @date: 2012-12-13   下午12:12:18 
 * @revision          $Id
 */
public class StandardTarget {

	private int pv;
	private int uv;
	private long darution; //停留时间
	private int visit;//访问数
	private int bounce;//跳出数
	private int rv;// 回访数
	private int uniVisitDepth; //去重访问深度
	
	
	public int getUniVisitDepth() {
		return uniVisitDepth;
	}
	public void setUniVisitDepth(int uniVisitDepth) {
		this.uniVisitDepth = uniVisitDepth;
	}
	public int getPv() {
		return pv;
	}
	public void setPv(int pv) {
		this.pv = pv;
	}
	public int getUv() {
		return uv;
	}
	public void setUv(int uv) {
		this.uv = uv;
	}
	public long getDarution() {
		return darution;
	}
	public void setDarution(long darution) {
		this.darution = darution;
	}
	public int getVisit() {
		return visit;
	}
	public void setVisit(int visit) {
		this.visit = visit;
	}
	public int getBounce() {
		return bounce;
	}
	public void setBounce(int bounce) {
		this.bounce = bounce;
	}
	public int getRv() {
		return rv;
	}
	public void setRv(int rv) {
		this.rv = rv;
	}

	
	

}

