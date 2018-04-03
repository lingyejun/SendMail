import contextlib
import json
import smtplib
import time
import urllib.request
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr

import pymysql

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO


# 定义上下文管理器，连接后自动关闭连接
@contextlib.contextmanager
def mysql(host='127.0.0.1', port=3306, user='db_name', passwd='db_pass', db='db_name', charset='utf8'):
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        yield cursor
    finally:
        conn.commit()
        cursor.close()
        conn.close()


# 格式化邮件地址
def formatAddr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))

def sendMail(body):
    smtp_server = 'smtp.163.com'
    user_name = 'your_name'
    from_mail = 'yourname@163.com'
    mail_pass = 'password'
    to_mail = ['test@163.com']
    # 构造一个MIMEMultipart对象代表邮件本身
    msg = MIMEMultipart()
    # Header对中文进行转码
    msg['From'] = formatAddr('OTA升级日报 <%s>' % from_mail)
    msg['To'] = ','.join(to_mail)
    msg['Subject'] = Header('OTA升级状态监测', 'utf-8')
    msg.attach(MIMEText(body, 'html', 'utf-8'))
    try:
        s = smtplib.SMTP()
        s.connect(smtp_server, "25")
        s.login(user_name, mail_pass)
        s.sendmail(from_mail, to_mail, msg.as_string())  # as_string()把MIMEText对象变成str
        s.quit()
    except smtplib.SMTPException as e:
        print(e)

def get_token(weixin_url, corpid, corpsecret):
    token_url = '%s/cgi-bin/gettoken?corpid=%s&corpsecret=%s' % (weixin_url, corpid, corpsecret)
    token = json.loads(urllib.request.urlopen(token_url).read().decode())['access_token']
    return token


def messages(msg):
    values = {
        "touser": '@all',
        "msgtype": 'text',
        "agentid": 1000014,
        "text": {'content': msg},
        "safe": 0
    }
    msges = (bytes(json.dumps(values), 'utf-8'))
    return msges


def send_message(weixin_url, token, data):
    send_url = '%s/cgi-bin/message/send?access_token=%s' % (weixin_url, token)
    respone = urllib.request.urlopen(urllib.request.Request(url=send_url, data=data)).read()
    x = json.loads(respone.decode())['errcode']
    # print(x)
    if x == 0:
        print('Succesfully')
    else:
        print('Failed')


if __name__ == '__main__':

    corpid = 'wx_corpid'
    corpsecret = 'wx_secret'
    weixin_url = 'https://qyapi.weixin.qq.com'

    web_tr_str = ''
    yes_web_tr_str = ''

    with mysql() as cursor:
        sql = "select device_type,device_model,x1.last_romversioin,x1.now_romversion,total_num,success_num,failed_num,concat(round(success_num/total_num * 100,2),'','%') as success_rate from (select device_type,device_model,t1.last_romversion as last_romversioin,t1.now_romversion as now_romversion,total_num,(case when t2.success_num is null then 0 else t2.success_num end) as success_num from (select device_type,device_model,last_romversion,now_romversion,count(device_id) as total_num from ota_upgrade_record group by device_type,device_model,last_romversion,now_romversion) as t1 left join (select last_romversion,now_romversion,count(device_id) as success_num from ota_upgrade_record where upgrade_status=1 group by last_romversion,now_romversion) as t2 on (t1.last_romversion=t2.last_romversion and t1.now_romversion=t2.now_romversion)) as x1,(select t1.last_romversion as last_romversioin,t1.now_romversion as now_romversion,(case when t2.failed_num is null then 0 else t2.failed_num end) as failed_num from (select last_romversion,now_romversion,count(device_id) as total_num from ota_upgrade_record group by last_romversion,now_romversion) as t1 left join (select last_romversion,now_romversion,count(device_id) as failed_num from ota_upgrade_record where upgrade_status=0 group by last_romversion,now_romversion) as t2 on (t1.last_romversion=t2.last_romversion and t1.now_romversion=t2.now_romversion)) as x2 where x1.last_romversioin=x2.last_romversioin and x1.now_romversion=x2.now_romversion order by device_type desc,total_num desc;"
        row_count = cursor.execute(sql)
        row_info = cursor.fetchall()
        for i in range(0, len(row_info)):
            for k in row_info[i].keys():
                row_tr_str = """
                <tr>
                """
                device_type = row_info[i]['device_type']
                device_model = row_info[i]['device_model']
                last_romversioin = row_info[i]['last_romversioin']
                now_romversion = row_info[i]['now_romversion']
                total_num = row_info[i]['total_num']
                success_num = row_info[i]['success_num']
                failed_num = row_info[i]['failed_num']
                success_rate = row_info[i]['success_rate']
                row_tr_str = row_tr_str + """<th>
                """ + str(device_type) + """</th>""" + """<th>""" + str(
                    device_model) + """</th>""" + """<th>""" + last_romversioin + """</th>""" + """<th>""" + now_romversion + """</th>""" + """<th>""" + str(
                    total_num) + """</th>""" + """<th>""" + str(success_num) + """</th>""" + """<th>""" + str(
                    failed_num) + """</th>""" + """<th>""" + success_rate + """</th>"""
                row_tr_str = row_tr_str + """
                </tr>
                """
            web_tr_str = web_tr_str + row_tr_str

    with mysql() as cursor:
        yesterday_sql = "select device_type,device_model,x1.last_romversioin,x1.now_romversion,total_num,success_num,failed_num,concat(round(success_num/total_num * 100,2),'','%') as success_rate from (select device_type,device_model,t1.last_romversion as last_romversioin,t1.now_romversion as now_romversion,total_num,(case when t2.success_num is null then 0 else t2.success_num end) as success_num from (select device_type,device_model,last_romversion,now_romversion,count(device_id) as total_num from ota_upgrade_record where create_time > UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) - INTERVAL 1 DAY) and create_time < UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)) group by device_type,device_model,last_romversion,now_romversion) as t1 left join (select last_romversion,now_romversion,count(device_id) as success_num from ota_upgrade_record where upgrade_status=1 and create_time > UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) - INTERVAL 1 DAY) and create_time < UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)) group by last_romversion,now_romversion) as t2 on (t1.last_romversion=t2.last_romversion and t1.now_romversion=t2.now_romversion)) as x1,(select t1.last_romversion as last_romversioin,t1.now_romversion as now_romversion,(case when t2.failed_num is null then 0 else t2.failed_num end) as failed_num from (select last_romversion,now_romversion,count(device_id) as total_num from ota_upgrade_record where create_time > UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) - INTERVAL 1 DAY) and create_time < UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)) group by last_romversion,now_romversion) as t1 left join (select last_romversion,now_romversion,count(device_id) as failed_num from ota_upgrade_record where upgrade_status=0 and create_time > UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) - INTERVAL 1 DAY) and create_time < UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE)) group by last_romversion,now_romversion) as t2 on (t1.last_romversion=t2.last_romversion and t1.now_romversion=t2.now_romversion)) as x2 where x1.last_romversioin=x2.last_romversioin and x1.now_romversion=x2.now_romversion order by device_type desc,total_num desc;"
        yesterday_row_count = cursor.execute(yesterday_sql)
        yesterday_row_info = cursor.fetchall()
        for i in range(0, len(yesterday_row_info)):
            for k in yesterday_row_info[i].keys():
                yes_row_tr_str = """
                        <tr>
                        """
                yes_device_type = yesterday_row_info[i]['device_type']
                yes_device_model = yesterday_row_info[i]['device_model']
                yes_last_romversioin = yesterday_row_info[i]['last_romversioin']
                yes_now_romversion = yesterday_row_info[i]['now_romversion']
                yes_total_num = yesterday_row_info[i]['total_num']
                yes_success_num = yesterday_row_info[i]['success_num']
                yes_failed_num = yesterday_row_info[i]['failed_num']
                yes_success_rate = yesterday_row_info[i]['success_rate']
                yes_row_tr_str = yes_row_tr_str + """<th>
                        """ + str(yes_device_type) + """</th>""" + """<th>""" + str(
                    yes_device_model) + """</th>""" + """<th>""" + yes_last_romversioin + """</th>""" + """<th>""" + yes_now_romversion + """</th>""" + """<th>""" + str(
                    yes_total_num) + """</th>""" + """<th>""" + str(yes_success_num) + """</th>""" + """<th>""" + str(
                    yes_failed_num) + """</th>""" + """<th>""" + yes_success_rate + """</th>"""
                yes_row_tr_str = yes_row_tr_str + """
                        </tr>
                        """
            yes_web_tr_str = yes_web_tr_str + yes_row_tr_str

    with mysql() as cursor:
        yes_total_sql = "select count(DISTINCT(device_id)) as device_upgrade_total_num,count(device_id) as total_upgrade_num from ota_upgrade_record where create_time > UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) - INTERVAL 1 DAY) and create_time < UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE));"
        row_count = cursor.execute(yes_total_sql)
        yes_total_row_info = cursor.fetchall()
        device_upgrade_total_num = yes_total_row_info[0]['device_upgrade_total_num']
        total_upgrade_num = yes_total_row_info[0]['total_upgrade_num']

    with mysql() as cursor:
        yes_success_sql = "select count(DISTINCT(device_id)) as device_upgrade_success_num,count(device_id) as success_upgrade_num from ota_upgrade_record where success_time > UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE) - INTERVAL 1 DAY) and success_time < UNIX_TIMESTAMP(CAST(SYSDATE()AS DATE));"
        row_count = cursor.execute(yes_success_sql)
        yes_success_row_info = cursor.fetchall()
        device_success_upd_num = yes_success_row_info[0]['device_upgrade_success_num']
        success_upd_num = yes_success_row_info[0]['success_upgrade_num']

        body = """
        <h1>OTA升级成功率汇总</h1>
        """

        table_str = '<table border="1">' + '\n' + '<tr>' + '\n' + '<th>产品类型</th><th>设备型号</th><th>开始版本</th><th>升级版本</th><th>设备总数量</th><th>升级成功设备数</th><th>待升级数量</th><th>升级成功率</th>' + '</tr>' + '\n'
        tody_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + '\n'
        yesterday_str = time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400)) + '\n'
        body = body + '<h2 style="color:#FF0000">' + tody_str + '截止，系统整体升级情况报告单' + '</h2>' + table_str + web_tr_str + '</table>' + '<h2 style="color:#FF0000"> 昨日' + yesterday_str + '升级情况报告单' + '</h2>' + table_str + yes_web_tr_str + '</table>'

        sendMail(body)
        test_token = get_token(weixin_url, corpid, corpsecret)
        wx_str1 = '昨天共受理' + str(device_upgrade_total_num) + '台设备发起的' + str(total_upgrade_num) + '次' + '升级请求' + '\n'
        wx_str2 = '其中有' + str(device_success_upd_num) + '台设备升级成功' + '\n'
        percent = "%.2f%%" % (device_success_upd_num / device_upgrade_total_num * 100)
        wx_str3 = '昨日一天的升级成功率为' + str(percent) + '\n'
        wx_body = wx_str1 + wx_str2 + wx_str3
        msg_data = messages(str(wx_body))
        print(weixin_url, test_token, msg_data)
        send_message(weixin_url, test_token, msg_data)

    # print(msg)
    print(body)
