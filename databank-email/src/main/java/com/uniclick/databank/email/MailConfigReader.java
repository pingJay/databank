package com.uniclick.databank.email;

import java.io.InputStream;


/**
 * 
*   
* 类名称：MailConfigReader  
* 类描述：  读取mail.properties配置文件
* 创建人：yang qi  
* 创建时间：2012-8-2 上午10:32:19  
* 修改人：yang qi  
* 修改时间：2012-8-2 上午10:32:19  
* 修改备注：  
* @version   
*
 */
public class MailConfigReader {
	 
	private static final String PROPERTIESPATH ="mail.properties";
	private String MAIL_SERVER_NAME = "";
	private String MAIL_SERVER_AUTH = "";
	private String MAIL_SERVER_ADDRESS = "";
	private String MAIL_SERVER_LOGIN_NAME = "";
	private String MAIL_SERVER_LOGIN_PASSWORD = "";
	private String MAIL_HTML_CODING = "";
	private String MIAL_SEND_TO_ADMIN = "" ;
	
	public MailConfigReader(){
			try {
				init();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	public void init() throws Exception{
	    java.util.Properties prop = new  java.util.Properties();
		InputStream in;
		in =Thread.currentThread().getContextClassLoader().getResourceAsStream(PROPERTIESPATH);
		prop.load(in);
		this.MAIL_SERVER_NAME = prop.getProperty("MAIL_SERVER_NAME");
		this.MAIL_HTML_CODING = prop.getProperty("MAIL_HTML_CODING");
		this.MAIL_SERVER_ADDRESS=prop.getProperty("MAIL_SERVER_ADDRESS");
		this.MAIL_SERVER_LOGIN_NAME = prop.getProperty("MAIL_SERVER_LOGIN_NAME");
		this.MAIL_SERVER_LOGIN_PASSWORD = prop.getProperty("MAIL_SERVER_LOGIN_PASSWORD");
		this.MAIL_SERVER_AUTH = prop.getProperty("MAIL_SERVER_AUTH");
		this.MIAL_SEND_TO_ADMIN = prop.getProperty("MIAL_SEND_TO_ADMIN");
	}
	
	public String readerMessage(String name){
		if("MAIL_SERVER_NAME".equals(name)){
			return this.MAIL_SERVER_NAME;
		}else if("MAIL_HTML_CODING".equals(name)){
			return this.MAIL_HTML_CODING;
		}else if("MAIL_SERVER_ADDRESS".equals(name)){
			return this.MAIL_SERVER_ADDRESS;
		}else if("MAIL_SERVER_LOGIN_NAME".equals(name)){
			return this.MAIL_SERVER_LOGIN_NAME;
		}else if("MAIL_SERVER_LOGIN_PASSWORD".equals(name)){
			return this.MAIL_SERVER_LOGIN_PASSWORD;
		}else if("MIAL_SEND_TO_ADMIN".equals(name)){
			return this.MIAL_SEND_TO_ADMIN;
		}else{
			return this.MAIL_SERVER_AUTH;
		}
		
	}
	
	public String[] readMsgAdmin(String name){
		String str = null; 
		String arr[] = null;
		if("MIAL_SEND_TO_ADMIN".equals(name)){
		    str = this.MIAL_SEND_TO_ADMIN;
		    if(str.contains(",")){
		      arr = str.split(",");
		    }else{
		      arr = new String[1];
		      arr[0]= str;
		    }
		}
		return arr;
	}
	

	public String getMAIL_SERVER_NAME() {
		return MAIL_SERVER_NAME;
	}

	public void setMAIL_SERVER_NAME(String mAIL_SERVER_NAME) {
		MAIL_SERVER_NAME = mAIL_SERVER_NAME;
	}

	public String getMAIL_SERVER_AUTH() {
		return MAIL_SERVER_AUTH;
	}

	public void setMAIL_SERVER_AUTH(String mAIL_SERVER_AUTH) {
		MAIL_SERVER_AUTH = mAIL_SERVER_AUTH;
	}

	public String getMAIL_SERVER_ADDRESS() {
		return MAIL_SERVER_ADDRESS;
	}

	public void setMAIL_SERVER_ADDRESS(String mAIL_SERVER_ADDRESS) {
		MAIL_SERVER_ADDRESS = mAIL_SERVER_ADDRESS;
	}

	public String getMAIL_SERVER_LOGIN_NAME() {
		return MAIL_SERVER_LOGIN_NAME;
	}

	public void setMAIL_SERVER_LOGIN_NAME(String mAIL_SERVER_LOGIN_NAME) {
		MAIL_SERVER_LOGIN_NAME = mAIL_SERVER_LOGIN_NAME;
	}

	public String getMAIL_SERVER_LOGIN_PASSWORD() {
		return MAIL_SERVER_LOGIN_PASSWORD;
	}

	public void setMAIL_SERVER_LOGIN_PASSWORD(String mAIL_SERVER_LOGIN_PASSWORD) {
		MAIL_SERVER_LOGIN_PASSWORD = mAIL_SERVER_LOGIN_PASSWORD;
	}

	public String getMAIL_HTML_CODING() {
		return MAIL_HTML_CODING;
	}

	public void setMAIL_HTML_CODING(String mAIL_HTML_CODING) {
		MAIL_HTML_CODING = mAIL_HTML_CODING;
	}

	public String getMIAL_SEND_TO_ADMIN() {
		return MIAL_SEND_TO_ADMIN;
	}

	public void setMIAL_SEND_TO_ADMIN(String mIAL_SEND_TO_ADMIN) {
		MIAL_SEND_TO_ADMIN = mIAL_SEND_TO_ADMIN;
	}
	
	public static void main(String args[]){
		MailConfigReader vo = new MailConfigReader();
		String [] arr = null;
		arr = vo.readMsgAdmin("MIAL_SEND_TO_ADMIN");
		for(int i = 0 ;i<arr.length;i++){
			System.out.println(arr[i]);
		}
	}

}
