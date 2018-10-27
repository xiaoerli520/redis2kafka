# coding=utf8


class Noticer:

    @staticmethod
    def send_email(mail_object, subject, to, content):
        print mail_object, subject, to, content

    @staticmethod
    def send_wehcat(wechat_object, subject, to, content):
        print wechat_object, subject, to ,content

