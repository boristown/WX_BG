# -*- coding: utf-8 -*-
# filename: prediction.py
import mysql.connector
import csv
import mypsw
import datetime

def get_prediction(symbol_id_list, prediction_file):
    with open(prediction_file,"r") as fcsv:
        csvreader = csv.reader(fcsv,delimiter = ",")
        predict_batch_size = len(list(csvreader))
        prices_batch_size = len(symbol_id_list)
        if predict_batch_size != prices_batch_size:
            print( "预测结果条目为：" + str(predict_batch_size) + "，与输入价格条目数:" + str(prices_batch_size) + "不匹配！" )
            return None
        mydb = mysql.connector.connect(
            host=mypsw.wechatadmin.host, 
            user=mypsw.wechatadmin.user, 
            passwd=mypsw.wechatadmin.passwd, 
            database=mypsw.wechatadmin.database, 
            auth_plugin='mysql_native_password')
        mycursor = mydb.cursor()
        insert_sql = "INSERT INTO predictions ("  \
            "SYMBOL, TIME, DAY01, DAY02, DAY03, DAY04,  DAY05, DAY06, DAY07, DAY08, DAY09, DAY10" \
            ") VALUES (" \
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        insert_val = []
        row_index = 0
        for row in csvreader:
            insert_val.append = (
                                 symbol_id_list[row_index], 
                                 datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                                 row[1], row[3], row[5], row[7], row[9], row[11], row[13], row[15], row[17], row[19]
                                 )
            row_index += 1

        if len(insert_val) == 0:
            print("读取预测结果失败！")
            return None

        mycursor.executemany(insert_sql, insert_val)

        mydb.commit()    # 数据表内容有更新，必须使用到该语句

        print(mycursor.rowcount, "记录插入成功。")

    return None