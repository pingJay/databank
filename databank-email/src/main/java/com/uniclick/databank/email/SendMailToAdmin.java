package com.uniclick.databank.email;

/**
 * 
*   
* 类名称：SendMailToAdmin  
* 类描述：  发送邮件给管理员
* 创建人：yang qi  
* 创建时间：2012-8-2 上午10:32:56  
* 修改人：yang qi  
* 修改时间：2012-8-2 上午10:32:56  
* 修改备注：  
* @version   
*
 */


public class SendMailToAdmin {
	
	MailConfigReader reader = new MailConfigReader();
	
	private String errorfrom = "yang.qi@uniclick.cn";
    private String[] errorto = reader.readMsgAdmin("MIAL_SEND_TO_ADMIN");
    private String errorsubject = "华扬联众AnalyseCenter后台数据统计系统报告";
    //private String errorcontent = "你好, 今天邮件发送失败,请核查";
      	
     public SendMailToAdmin(){
     }
     
     public void init(String errorcontent){
    	 SendMailToUser vo = new SendMailToUser();
    	 try {
			vo.execMailToUser(this.errorfrom, this.errorto, null, null, this.errorsubject, errorcontent, null,"");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     }

	public String getErrorfrom() {
		return errorfrom;
	}

	public void setErrorfrom(String errorfrom) {
		this.errorfrom = errorfrom;
	}

	public String[] getErrorto() {
		return errorto;
	}

	public void setErrorto(String[] errorto) {
		this.errorto = errorto;
	}

	public String getErrorsubject() {
		return errorsubject;
	}

	public void setErrorsubject(String errorsubject) {
		this.errorsubject = errorsubject;
	}

/*	public String getErrorcontent() {
		return errorcontent;
	}

	public void setErrorcontent(String errorcontent) {
		this.errorcontent = errorcontent;
	}*/
	
	
	public static void main(String args[]){
		SendMailToAdmin send = new SendMailToAdmin();
		send.init("测试邮件");
		
	}
}
