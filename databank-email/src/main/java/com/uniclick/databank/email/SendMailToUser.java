package com.uniclick.databank.email;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;

/**
 * 
*   
* 类名称：SendMailToUser  
* 类描述：  发送邮件给用户
* 创建人：yang qi  
* 创建时间：2012-8-2 上午10:34:04  
* 修改人：yang qi  
* 修改时间：2012-8-2 上午10:34:04  
* 修改备注：  
* @version   
*
 */


public class SendMailToUser {

    public SendMailToUser() {
        // empty
    }

    /**
     * 执行邮件处理过程(包含单个附件)
     * 
     * @param from
     *            String 发件人
     * @param to
     *            String[] 收件人数组
     * @param cc
     *            String[] 抄送人数组
     * @param bcc
     *            String[] 暗抄送人数组
     * @param subject
     *            String 主题
     * @param contest
     *            String 内容
     * @param address
     *            String 附件地址
     * @param name
     *            String 附件名
     */
    public  void execMailToUser(String from, String[] to, String[] cc,
            String[] bcc, String subject, String content, String address,
            String name)  {
        try {
            // 设置邮件编码格式
            String mailType = MailConstants.getValue("MAIL_HTML_CODING");

            checkParameter(from, to, cc, bcc, subject);

            // 配置邮件参数
            Properties mailProps = System.getProperties();

            // 配置邮件服务器地址
            mailProps.put(MailConstants.getValue("MAIL_SERVER_NAME"), MailConstants
                    .getValue("MAIL_SERVER_ADDRESS"));
            mailProps.put(MailConstants.getValue("MAIL_SERVER_AUTH"), "true");
            Session mailSession = Session.getDefaultInstance(mailProps, null);

            // 配置邮件内容
            MimeMessage message = new MimeMessage(mailSession);

            message.setFrom(new InternetAddress(from));// 设置发件人
            
            InternetAddress[] toList = new InternetAddress[to.length];
            for (int i = 0; i < to.length; i++) {
            	toList[i] = new InternetAddress(to[i]);
            }
            message.setRecipients(Message.RecipientType.TO,
            		toList);// 设置收件人

            message.setSubject(subject);// 邮件标题
            if (cc != null) {
                for (int i = 0; i < cc.length; i++) {
                    message.setRecipient(Message.RecipientType.CC,
                            new InternetAddress(cc[i]));// 设置抄送人
                }
            }

            if (bcc != null) {
                for (int i = 0; i < bcc.length; i++) {
                    message.setRecipient(Message.RecipientType.BCC,
                            new InternetAddress(bcc[i]));// 设置暗抄送人
                }
            }
            if (null != address && !"".equals(address)) {
                // 配置附件信息
                MimeMultipart multi = new MimeMultipart();
                BodyPart textBodyPart = new MimeBodyPart();
                textBodyPart.setContent(content, mailType);
                multi.addBodyPart(textBodyPart);
                FileDataSource fds = new FileDataSource(address);
                BodyPart fileBodyPart = new MimeBodyPart();
                fileBodyPart.setDataHandler(new DataHandler(fds));
                fileBodyPart.setFileName(MimeUtility.encodeText(name));

                multi.addBodyPart(fileBodyPart);
                message.setContent(multi);
                message.setSubject(subject);
                message.saveChanges();
            } else {
                message.setContent(content, mailType);
            }
            // 发送邮件
            Transport transport = mailSession.getTransport("smtp");
            transport.connect(MailConstants.getValue("MAIL_SERVER_ADDRESS"),
                    MailConstants.getValue("MAIL_SERVER_LOGIN_NAME"), MailConstants
                            .getValue("MAIL_SERVER_LOGIN_PASSWORD"));
            transport.sendMessage(message, message.getAllRecipients());
            transport.close();

        } catch (Exception e) {
        	new SendMailToAdmin();
        }
    }

    /**
     * 执行邮件处理过程(包含多个附件)
     * 
     * @param from
     *            String 发件人
     * @param to
     *            String[] 收件人数组
     * @param cc
     *            String[] 抄送人数组
     * @param bcc
     *            String[] 暗抄送人数组
     * @param subject
     *            String 主题
     * @param contest
     *            String 内容
     * @param address
     *            String[] 附件地址
     * @param name
     *            String[] 附件名
     */
    public  void execMailToUser(String from, String[] to, String[] cc,
            String[] bcc, String subject, String contest, String[] address,
            String[] name)  {
        try {
            // 设置邮件编码格式
            String mailType = MailConstants.getValue("MAIL_HTML_CODING");

            checkParameter(from, to, cc, bcc, subject);

            // 配置邮件参数
            Properties mailProps = System.getProperties();

            // 配置邮件服务器地址
            mailProps.put(MailConstants.getValue("MAIL_SERVER_NAME"), MailConstants
                    .getValue("MAIL_SERVER_ADDRESS"));
            mailProps.put(MailConstants.getValue("MAIL_SERVER_AUTH"), "true");
            Session mailSession = Session.getDefaultInstance(mailProps, null);

            // 配置邮件内容
            MimeMessage message = new MimeMessage(mailSession);

            message.setFrom(new InternetAddress(from));// 设置发件人

            for (int i = 0; i < to.length; i++) {
                message.setRecipient(Message.RecipientType.TO,
                        new InternetAddress(to[i]));// 设置收件人
            }

            message.setSubject(subject);// 邮件标题
            if (cc != null) {
                for (int i = 0; i < cc.length; i++) {
                    message.setRecipient(Message.RecipientType.CC,
                            new InternetAddress(cc[i]));// 设置抄送人
                }
            }

            if (bcc != null) {
                for (int i = 0; i < bcc.length; i++) {
                    message.setRecipient(Message.RecipientType.BCC,
                            new InternetAddress(bcc[i]));// 设置暗抄送人
                }
            }
            if (null != address) {
                // 配置附件信息
                MimeMultipart multi = new MimeMultipart();
                BodyPart textBodyPart = new MimeBodyPart();
                textBodyPart.setText(contest);
                multi.addBodyPart(textBodyPart);

                for (int i = 0; i < address.length; i++) {
                    FileDataSource fds = new FileDataSource(address[i]);
                    BodyPart fileBodyPart = new MimeBodyPart();
                    fileBodyPart.setDataHandler(new DataHandler(fds));
                    fileBodyPart.setFileName(MimeUtility.encodeText(name[i]));
                    multi.addBodyPart(fileBodyPart);
                }

                message.setContent(multi, mailType);
                message.setSubject(subject);
                message.saveChanges();
            } else {
                // message.setText(contest);
                message.setText(contest, mailType);
            }
            // 发送邮件
            Transport transport = mailSession.getTransport("smtp");
            transport.connect(MailConstants.getValue("MAIL_SERVER_ADDRESS"),
                    MailConstants.getValue("MAIL_SERVER_LOGIN_NAME"), MailConstants
                            .getValue("MAIL_SERVER_LOGIN_PASSWORD"));
            transport.sendMessage(message, message.getAllRecipients());
            transport.close();

        } catch (Exception e) {
        	new SendMailToAdmin();
        }
    }

    /**
     * 判断输入参数合法性
     * 
     * @param from
     *            String 发件人
     * @param to
     *            String[] 收件人数组
     * @param cc
     *            String[] 抄送人数组
     * @param bcc
     *            String[] 暗抄送人数组
     * @param subject
     *            String 主题
     * @throws Exception
     *             参数异常
     */
    private static void checkParameter(String from, String[] to, String[] cc,
            String[] bcc, String subject) throws Exception {
        if (!executeDatabase(from)) {
            throw new Exception(MailConstants.ERROR_MAIL_FROM + from);
        }

        for (int i = 0; i < to.length; i++) {

            if (!executeDatabase(to[i])) {
                throw new Exception(MailConstants.ERROR_MAIL_TO + to[i]);
            }
        }
        if (null != cc) {
            for (int i = 0; i < cc.length; i++) {
                if (!executeDatabase(cc[i]) && null != cc[i]) {
                    throw new Exception(MailConstants.ERROR_MAIL_CC + cc[i]);
                }
            }
        }
        if (null != bcc) {
            for (int i = 0; i < bcc.length; i++) {
                if (!executeDatabase(bcc[i]) && null != bcc[i]) {
                    throw new Exception(MailConstants.ERROR_MAIL_BCC + bcc[i]);
                }
            }
        }
        
        
        
        if ("" == subject) {
            throw new Exception(MailConstants.ERROR_SUBJECT_NULL);
        }
    }

    /**
     * 判断邮件地址
     * 
     * @param address
     *            String 邮件地址
     * @return boolean 输出邮件是否合理 true：合理 false:不合理
     */
    private static boolean executeDatabase(String address) {
        Pattern p = Pattern.compile("\\w+.*\\w*@(\\w+\\.)+[a-z]{2,3}");
        Matcher m = p.matcher(address.trim());
        if (!m.matches()) {
            return false;
        }

        return true;
    }
    
    
    public static void main(String [] arg) {
    	SendMailToUser sendMail = new SendMailToUser();
    	String from = MailConstants.MAIL_FROM;
        String[] to = new String[]{"ping.jie@uniclick.cn"};
        String subject = "华扬联众系统报告";
        String content = "测试邮件";
        String att_addr = "D:\\data\\databank\\excel\\company/ComapnyReport_测试新代码.xls";
        String att_name = "报告附件.txt";
        sendMail.execMailToUser(from, to, null, null, subject, content,att_addr, "测试新代码.xls");
        //SendMailToUser.execMailToUser(from, to, null, null, subject, content, null, "");
			// TODO Auto-generated catch block
    }

}
