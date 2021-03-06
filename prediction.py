# -*- coding: utf-8 -*-
# filename: prediction.py
import mysql.connector
import csv
import mypsw
import datetime
import time

def get_prediction(symbol_id_list, prediction_file):
    with open(prediction_file,"r") as fcsv:
        csvreader = csv.reader(fcsv)
        prices_batch_size = len(symbol_id_list)
        insert_val = []
        predict_index = 0
        print("准备读取预测结果……")
        time.sleep(5)
        #for linetext in fcsv:
        for row in csvreader:
            #row = linetext.split(',')
            insert_val.append((
                                 symbol_id_list[predict_index][0], 
                                 symbol_id_list[predict_index][1], 
                                 #datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                                 #row[1], row[3], row[5], row[7], row[9], row[11], row[13], row[15], row[17], row[19]
                                 row[1], row[1], row[1], row[1], row[1], row[1], row[1], row[1], row[1], row[1]
                                 ))
            predict_index += 1

        if predict_index != prices_batch_size:
            print( "预测结果条目为：" + str(predict_index) + "，与输入价格条目数:" + str(prices_batch_size) + "不匹配！" )
            return None

        if len(insert_val) == 0:
            print("读取预测结果失败！")
            return None

        try:
            mydb = mysql.connector.connect(
                host=mypsw.wechatadmin.host, 
                user=mypsw.wechatadmin.user, 
                passwd=mypsw.wechatadmin.passwd, 
                database=mypsw.wechatadmin.database, 
                auth_plugin='mysql_native_password')
            mycursor = mydb.cursor()
        except:
            print("数据库连接失败！")
            return None

        insert_sql = "INSERT INTO predictions ("  \
            "SYMBOL, TIME, DAY01, DAY02, DAY03, DAY04,  DAY05, DAY06, DAY07, DAY08, DAY09, DAY10" \
            ") VALUES (" \
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
            "ON DUPLICATE KEY UPDATE TIME=VALUES(TIME)," \
            "DAY01=VALUES(DAY01)," \
            "DAY02=VALUES(DAY02)," \
            "DAY03=VALUES(DAY03)," \
            "DAY04=VALUES(DAY04)," \
            "DAY05=VALUES(DAY05)," \
            "DAY06=VALUES(DAY06)," \
            "DAY07=VALUES(DAY07)," \
            "DAY08=VALUES(DAY08)," \
            "DAY09=VALUES(DAY09)," \
            "DAY10=VALUES(DAY10)" \

        mycursor.executemany(insert_sql, insert_val)

        mydb.commit()    # 数据表内容有更新，必须使用到该语句

        print(mycursor.rowcount, "记录更新成功。")

    return None

def get_predictionhistory(symbol_id_list, prediction_file, prediction_file_second):
    with open(prediction_file,"r") as fcsv:
        with open(prediction_file_second,"r") as fcsv_second:
            csvreader = csv.reader(fcsv)
            csvreader_second = csv.reader(fcsv_second)
            prices_batch_size = len(symbol_id_list)
            update_val = []
            second_val = []
            predict_index = 0
            print("准备读取预测结果……")
            time.sleep(5)
            #for linetext in fcsv:
            for row in csvreader_second:
                #row = linetext.split(',')
                second_val.append(float(row[1]))

            for row in csvreader:
                #row = linetext.split(',')
                update_val.append((
                                     float(row[1]),
                                     second_val[predict_index],
                                     datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                                     symbol_id_list[predict_index][0], 
                                     symbol_id_list[predict_index][1], 
                                     ))
                predict_index += 1

            if predict_index != prices_batch_size:
                print( "预测结果条目为：" + str(predict_index) + "，与输入价格条目数:" + str(prices_batch_size) + "不匹配！" )
                return None

            if len(update_val) == 0:
                print("读取预测结果失败！")
                return None

            try:
                mydb = mysql.connector.connect(
                    host=mypsw.wechatadmin.host, 
                    user=mypsw.wechatadmin.user, 
                    passwd=mypsw.wechatadmin.passwd, 
                    database=mypsw.wechatadmin.database, 
                    auth_plugin='mysql_native_password')
                mycursor = mydb.cursor()
            except:
                print("数据库连接失败！")
                return None

            update_sql = "UPDATE pricehistory SET F = %s, F7 = %s, PREDICTTIME = %s where SYMBOL = %s and date = %s"

            try:
                mycursor.executemany(update_sql, update_val)
                mydb.commit()    # 数据表内容有更新，必须使用到该语句
            except Exception as e:
                print("数据库连接失败：" + str(e))
                return None

            print(mycursor.rowcount, "记录更新成功。")

    return None
