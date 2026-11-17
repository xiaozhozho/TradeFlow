import smtplib
from .log import get_logger
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

from ..setting import EMAIL_SERVER,EMAIL_PORT,EMAIL_SENDER,EMAIL_PASSWORD,EMAIL_REVEIVER,EMAIL_USERNAME

logger = get_logger()


class Email:
    """邮件发送模块"""
    def __init__(self) -> None:
        self.server = EMAIL_SERVER
        self.port = EMAIL_PORT
        self.sender = EMAIL_SENDER
        self.password = EMAIL_PASSWORD
        self.receiver = EMAIL_REVEIVER
        self.username = EMAIL_USERNAME

    def sendText(self, subject='', msg=''):
        """发送文字邮件"""
        message = MIMEText(msg,'plain','utf-8')   
        message['Subject'] = subject
        message['From'] = self.sender
        message['To'] = self.receiver
        try:
            server=smtplib.SMTP_SSL(self.server, self.port)
            server.login(self.sender,self.password)
            server.sendmail(self.sender, self.receiver, message.as_string())
            server.quit()
            logger.info(f"Send text email to {self.receiver} success!")
        except smtplib.SMTPException as e:
            logger.info(f"<Email>: ERROR:{e}")


    def sendImage(self, subject='', msg='', url=''):
        """发送包含图片邮件"""
        message =  MIMEMultipart('related')
        message['Subject'] = subject
        message['From'] = self.sender
        message['To'] = self.receiver
        content = MIMEText(f'<html><body><img src="cid:imageid" alt="imageid">{msg}</body></html>','html','utf-8')
        message.attach(content)
        
        file=open(url, "rb")
        data = file.read()
        file.close()
        img = MIMEImage(data)
        img.add_header('Content-ID', 'imageid')
        message.attach(img)

        try:
            server=smtplib.SMTP_SSL(self.server, self.port)
            server.login(self.sender,self.password)
            server.sendmail(self.sender, self.receiver, message.as_string())
            server.quit()
            logger.info(f"Send image email to {self.receiver} success!")
        except smtplib.SMTPException as e:
            logger.info(f"<Email>: ERROR:{e}")
