# Python 3.8.0
import smtplib
import time
import imaplib
import email
import traceback 
import pandas as pd
import pymysql
from sqlalchemy import create_engine
# -------------------------------------------------
#
# Utility to read email from Gmail Using Python
#
# ------------------------------------------------

import sched
s = sched.scheduler(time.time, time.sleep)

# ------------------------------------------------
# 建立sql連結
engine = create_engine("mysql+pymysql://{user}:{pw}@database-1.c6nb76zfkw27.us-east-2.rds.amazonaws.com/{db}"
                       .format(user="admin",
                               pw="nqtradenotification1234Secret",
                               db="nqtrade"))
# ------------------------------------------------

ORG_EMAIL = "@gmail.com" 
FROM_EMAIL = "nqtradenotification2" + ORG_EMAIL 
FROM_PWD = "nqtradenotification1234" 
SMTP_SERVER = "imap.gmail.com" 
SMTP_PORT = 993

def read_email_from_gmail(sc):
    # 每60秒循環一次
    s.enter(60, 1, read_email_from_gmail, (sc,))

    try:
        mail = imaplib.IMAP4_SSL(SMTP_SERVER)
        mail.login(FROM_EMAIL,FROM_PWD)
        mail.select('inbox')

        data = mail.search(None, 'ALL')
        mail_ids = data[1]
        id_list = mail_ids[0].split()   
        first_email_id = int(id_list[0])
        latest_email_id = int(id_list[-1])

        # 顯示目前的email數量
        print(latest_email_id)

        # -------------------- 分割線 --------------------
        # 讀取資料庫中的email_id
        email_id_in_database = pd.read_sql("select * from nqtrade.email_id3",con = engine)
        email_id_in_database=email_id_in_database['email_id'].iloc[-1]

        if latest_email_id > email_id_in_database:

          email_content=[]
          # 重點 - 只要將first_email_id set做上一次最後的email_id，get的時候就不用拿到全部email
          for i in range(latest_email_id,email_id_in_database, -1):          
              data = mail.fetch(str(i), '(RFC822)' )
              for response_part in data:
                  arr = response_part[0]
                  if isinstance(arr, tuple):
                      msg = email.message_from_string(str(arr[1],'utf-8'))
                      email_subject = msg['subject']
                      # email_from = msg['from']
                      # print('From : ' + email_from + '\n')
                      if email_subject=='trade':
                        # print('Subject : ' + email_subject + '\n')
                        email_content.append(msg.get_payload())
                        # print(str(msg.get_payload()))
                      # 提取payload形成dataframe的代码
          email_content=pd.DataFrame(email_content)
          print(email_content)
          new = email_content[0].str.split(',', n = 1, expand = True) 
          print(new)
          email_content["c"]= new[0] 
          email_content["d"]= new[1]
          new = email_content["d"].str.split(',', n = 1, expand = True) 
          email_content["c1"]= new[0] 
          email_content["d1"]= new[1]
          new = email_content["d1"].str.split(',', n = 1, expand = True) 
          email_content["c2"]= new[0] 
          email_content["d2"]= new[1]
          new = email_content["d2"].str.split(',', n = 1, expand = True) 
          email_content["c3"]= new[0] 
          email_content["d3"]= new[1]
          new = email_content["d3"].str.split('\r\n', n = 1, expand = True) 
          email_content["c4"]= new[0] 
          email_content["d4"]= new[1]          
          email_content2=email_content[['c','c1','c2','c3','c4']]
          email_content2.columns=['日期','价钱','方向','状态','种类']
          email_content2=email_content2[['日期','价钱','方向','状态','种类']]
          # 上傳更新的數據到Database
          print(email_content2)
          email_content2.to_sql('track_trades2', con = engine, if_exists = 'append', chunksize = 1000, index=False)

          # 上傳完畢後更新資料庫中的email_id
          if latest_email_id > email_id_in_database:
            update_email_id=[latest_email_id]
            update_email_id=pd.DataFrame(update_email_id)
            update_email_id.columns=['email_id']
            update_email_id.to_sql('email_id3', con = engine, if_exists = 'append', chunksize = 1000, index=False)

    except Exception as e:
        traceback.print_exc() 
        print(str(e))

# read_email_from_gmail()
s.enter(60, 1, read_email_from_gmail, (s,))
s.run()