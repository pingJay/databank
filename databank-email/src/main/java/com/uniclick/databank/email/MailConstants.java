package com.uniclick.databank.email;

import java.util.HashMap;
import java.util.Map;

/***
 * 
*   
* 类名称：Constants  
* 类描述：  邮件错误类型
* 创建人：yang qi  
* 创建时间：2012-8-2 上午10:30:54  
* 修改人：yang qi  
* 修改时间：2012-8-2 上午10:30:54  
* 修改备注：  
* @version   
*
 */

public class MailConstants {
   
    public static int FOCUS = 20;
  
    /**
     * 系统发件人
     */
    public static String MAIL_FROM = "tracking@uniclick.cn";
    /**
     * 邮件附件格式不对。
     */
    public static String ERROR_MAIL_ATTACH = "邮件附件格式不对";

    /**
     * 邮件附件格式不对。
     */
    public static String ERROR_MAIL_ACTION = "邮件附件发送方式选取不正确";

    /**
     * 发件人地址错误。
     */
    public static String ERROR_MAIL_FROM = "发件人地址错误";

    /**
     * 收件人地址错误。
     */
    public static String ERROR_MAIL_TO = "收件人地址错误";

    /**
     * 抄送人地址错误。
     */
    public static String ERROR_MAIL_CC = "抄送人地址错误";

    /**
     * 暗抄送人地址错误。
     */
    public static String ERROR_MAIL_BCC = "暗抄送人地址错误";

    /**
     * 主题不能为空。
     */
    public static String ERROR_SUBJECT_NULL = "主题不能为空";

    public static int ERROR_RECOVERED = 1;
    public static int ERROR_NOT_RECOVERED = 0;

    /**
     * 参数配置Map
     */
    private static Map<String, String> configMap = new HashMap<String, String>(); 

    
    
    public static String getValue(String name) throws Exception {
    
       if (MailConstants.configMap.isEmpty()) {
          MailConfigReader reader = new MailConfigReader();
          return reader.readerMessage(name);
          
       }
       
       return "";
  
    }

    
}