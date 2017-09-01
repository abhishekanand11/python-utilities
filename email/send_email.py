#--------- Script used to send email --------------------------------------------
__author__ = 'abhishekanand11'


import os
import email
import sys
import email.mime.application
import smtplib
from email.mime.text import MIMEText
import csv

#-------------------------- Server Details ------------------------

smtpserver = 'smtp server address'
username = 'username'
password = 'password'
sender_name = 'abhishek.anand@gmail.com'

recipients = ['abhishek.anand@gmail.com', 'abhishek@abc.com']
emailMsg = email.MIMEMultipart.MIMEMultipart()
emailMsg['Subject'] = 'Email Subject - '
emailMsg['From'] = sender_name
emailMsg['To'] = ', '.join(recipients)

def main():
    with open(file_name, 'rb') as fp:
            att = email.mime.application.MIMEApplication(fp.read(), _subtype="csv")
    att.add_header('Content-Disposition', 'attachment', filename=file_name)
    emailMsg.attach(att)
    send_email()

def send_email():
    server = smtplib.SMTP(smtpserver)
    server.starttls()
    server.login(username, password)
    server.sendmail(sender_name, recipients, emailMsg.as_string())
    server.quit()
    
    
    
if __name__ == '__main__':
    main()