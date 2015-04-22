/**
 * @Title: MapReduceJobOptionModel.java
 * @Package com.uniclick.databank.mapreduce.pojo
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-13 下午7:26:15
 * @version V1.0
 */

package com.uniclick.databank.mapreduce.pojo;

/**
 * @ClassName: MapReduceJobOptionModel
 * @Description: TODO(MR任务 接受的参数)
 * @author ping.jie
 * @date 2015-4-13 下午7:26:15
 * @verision $Id
 * 
 */

public class MapReduceJobOptionModel {
	private String act = "";
	private String order = "";
	private String company = "";
	private String urltype = "";
	private String site = "";
	private String inventory = "";
	private String frequency = "";
	private String collect = "";	
	private String coincide = "";
	private String orderbegion = "";
	private String orderenddate = "";
	
	/**
	 * @return act
	 */
	
	public String getAct() {
		return replace(act);
	}
	/**
	 * @param act 要设置的 act
	 */
	
	public void setAct(String act) {
		this.act = act;
	}
	/**
	 * @return order
	 */
	
	public String getOrder() {
		return replace(order);
	}
	/**
	 * @param order 要设置的 order
	 */
	
	public void setOrder(String order) {
		this.order = order;
	}
	/**
	 * @return company
	 */
	
	public String getCompany() {
		return replace(company);
	}
	/**
	 * @param company 要设置的 company
	 */
	
	public void setCompany(String company) {
		this.company = company;
	}
	/**
	 * @return urltype
	 */
	
	public String getUrltype() {
		return replace(urltype);
	}
	/**
	 * @param urltype 要设置的 urltype
	 */
	
	public void setUrltype(String urltype) {
		this.urltype = urltype;
	}
	/**
	 * @return site
	 */
	
	public String getSite() {
		return replace(site);
	}
	/**
	 * @param site 要设置的 site
	 */
	
	public void setSite(String site) {
		this.site = site;
	}
	/**
	 * @return inventory
	 */
	
	public String getInventory() {
		return replace(inventory);
	}
	/**
	 * @param inventory 要设置的 inventory
	 */
	
	public void setInventory(String inventory) {
		this.inventory = inventory;
	}
	/**
	 * @return frequency
	 */
	
	public String getFrequency() {
		return replace(frequency);
	}
	/**
	 * @param frequency 要设置的 frequency
	 */
	
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	/**
	 * @return collect
	 */
	
	public String getCollect() {
		return replace(collect);
	}
	/**
	 * @param collect 要设置的 collect
	 */
	
	public void setCollect(String collect) {
		this.collect = collect;
	}
	/**
	 * @return coincide
	 */
	
	public String getCoincide() {
		return replace(coincide);
	}
	/**
	 * @param coincide 要设置的 coincide
	 */
	
	public void setCoincide(String coincide) {
		this.coincide = coincide;
	}
	
	
	/**
	 * @return orderbegion
	 */
	
	public String getOrderbegion() {
		return replace(orderbegion);
	}
	/**
	 * @param orderbegion 要设置的 orderbegion
	 */
	
	public void setOrderbegion(String orderbegion) {
		this.orderbegion = orderbegion;
	}
	/**
	 * @return orderenddate
	 */
	
	public String getOrderenddate() {
		return replace(orderenddate);
	}
	/**
	 * @param orderenddate 要设置的 orderenddate
	 */
	
	public void setOrderenddate(String orderenddate) {
		this.orderenddate = orderenddate;
	}
	private String replace(String str) {
		return str.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\"", "");
	}
}
