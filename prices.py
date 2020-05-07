# -*- coding: utf-8 -*-
# filename: prices.py
import mysql.connector
import mypsw
import os
import re
import requests
import datetime
import time
import math

def read_prices():
    try:
        mydb = mysql.connector.connect(host=mypsw.wechatadmin.host, 
            user=mypsw.wechatadmin.user, 
            passwd=mypsw.wechatadmin.passwd, 
            database=mypsw.wechatadmin.database, 
            auth_plugin='mysql_native_password')
        mycursor = mydb.cursor()
    except:
        print("数据库连接失败！")
        return None

    #Load All prices 
    select_alias_statment = "SELECT * FROM price where price001 != price002 order by RAND()"
    mycursor.execute(select_alias_statment)
    try:
        #alias_results = mycursor.fetchall()
        price_results = mycursor.fetchall()
    except:
        print("数据库连接失败！")
        return None
    #if len(alias_results) == 0:
    if len(price_results) == 0:
        #ret_message = "市场表symbol_alias无数据！请先维护！" 
        print("价格表price无数据！请先运行爬虫程序prices.py")
        return None
    
    inputdays = 120


    symbol_index = 0
    time_text =  datetime.datetime.utcnow().strftime("%Y%m%d")
    price_filename_txt = os.path.join('Output/prices/part-WX_' + time_text + '.txt')
    price_filename_csv = os.path.join('Output/prices/price-WX_' + time_text + '.csv')
    price_file = open(price_filename_txt, "w", encoding="utf-8")
    price_file.truncate()
    symbol_id_list = []
    # for alias_result in alias_results:
    for price_result in price_results:
        #if alias_result[1] == '外汇':
        #  print(price_list)
        price_list = []
        for price_index in range(inputdays):
            price_list.append(float(price_result[price_index+2]))
        max_price = max(price_list)
        min_price = min(price_list)
        center_price = (max_price + min_price) / 2
        range_price = max_price - min_price
        if range_price <= 0:
            continue
        #symbol_id_list.append(alias_result[0])
        symbol_id_list.append((price_result[0], price_result[1]))
        symbol_index+=1
        if symbol_index > 1:
            price_file.write("\n")
        price_line = ""
        #print( "%d\t%s" % (symbol_index, alias_result[0])  )
        for i in range(len(price_list)):
            price_list[i] -= center_price
            price_list[i] /= range_price
            price_list[i] += 0.5
            if price_list[i] > 0.99999:
                price_list[i] = 1.0
            elif price_list[i] < 0.00001:
                price_list[i] = 0.0
            if price_line != "":
                price_line += ","
            price_line += str(price_list[i])
        price_file.write(price_line)
        #print(price_list)
    price_file.close()
    os.rename(price_filename_txt, price_filename_csv)

    return symbol_id_list

def read_pricehistory(predict_batch_size):
    try:
        mydb = mysql.connector.connect(host=mypsw.wechatadmin.host, 
            user=mypsw.wechatadmin.user, 
            passwd=mypsw.wechatadmin.passwd, 
            database=mypsw.wechatadmin.database, 
            auth_plugin='mysql_native_password')
        mycursor = mydb.cursor()
    except Exception as e:
        print("数据库连接失败：" + e)
        return None

    db_offset = 0

    #Load All prices 
    #select_symbol_statment = "select symbol, predictdate FROM predictlog order by predictdate asc"
    select_symbol_statment = "select symbol, predictdate, loadingdate FROM predictlog order by rand()"
    mycursor.execute(select_symbol_statment)
    try:
        #alias_results = mycursor.fetchall()
        symbols_results = mycursor.fetchall()
    except Exception as e:
        print("数据库连接失败！" + e)
        return None

    #if len(alias_results) == 0:
    if len(symbols_results) == 0:
        #ret_message = "市场表symbol_alias无数据！请先维护！" 
        print("predictlog表无数据！")
        return None
    
    inputdays = 120


    symbol_index = 0
    time_text =  datetime.datetime.utcnow().strftime("%Y%m%d")
    price_filename_txt = os.path.join('Output/prices/part-WX_' + time_text + '.txt')
    price_filename_csv = os.path.join('Output/prices/price-WX_' + time_text + '.csv')
    price_file = open(price_filename_txt, "w", encoding="utf-8")
    price_file.truncate()
    symbol_id_list = []
    # for alias_result in alias_results:
    for symbol_results in symbols_results:
        #if alias_result[1] == '外汇':
        #  print(price_list)
        sec =int(time.time())
        if sec % 5 == 0:
            datefield = 'loadingdate'
            datefieldindex = 2
        else:
            datefield = 'predictdate'
            datefieldindex = 1
        select_price_statment = "select * FROM pricehistory where symbol = '" + symbol_results[0] + "' and date >= '"  + symbol_results[datefieldindex].strftime("%Y-%m-%d") + "'  order by symbol, date"
        mycursor.execute(select_price_statment)

        try:
            prices_results = mycursor.fetchall()
        except:
            print("symbol:" + symbol_results[0] + ", no data!")
            continue

        predict_count = len(prices_results) - inputdays + 1
        if predict_count <= 0:
            update_val = []
            update_sql = "UPDATE predictlog SET " + datefield + " = %s  where SYMBOL = %s "
            update_val.append((prices_results[-1][1], symbol_results[0]))

            mycursor.executemany(update_sql, update_val)

            mydb.commit()    # 数据表内容有更新，必须使用到该语句
            continue

        #for price_results in prices_results:
        for predict_index in range(predict_count):
            price_list = []
            for price_index in range(inputdays):
                price_list.append(float(prices_results[predict_index + inputdays - 1 - price_index][3]))
                price_list.append(float(prices_results[predict_index + inputdays - 1 - price_index][5]))
                price_list.append(float(prices_results[predict_index + inputdays - 1 - price_index][4]))
            price_list = [math.log(price) if price > 0 else 0 for price in price_list]
            max_price = max(price_list)
            min_price = min(price_list)
            center_price = (max_price + min_price) / 2
            range_price = max_price - min_price
            if range_price <= 0:
                continue
            #symbol_id_list.append(alias_result[0])
            symbol_id_list.append((prices_results[predict_index + inputdays - 1][0], prices_results[predict_index + inputdays - 1][1]))
            symbol_index+=1
            if symbol_index > 1:
                price_file.write("\n")
            price_line = ""
            #print( "%d\t%s" % (symbol_index, alias_result[0])  )
            for i in range(len(price_list)):
                price_list[i] -= center_price
                price_list[i] /= range_price
                price_list[i] += 0.5
                if price_list[i] > 0.99999:
                    price_list[i] = 1.0
                elif price_list[i] < 0.00001:
                    price_list[i] = 0.0
                if price_line != "":
                    price_line += ","
                price_line += str(price_list[i])
            price_file.write(price_line)
            
            update_val = []

            update_sql = "UPDATE predictlog SET " + datefield + " = %s, maxdate = %s where SYMBOL = %s "
            update_val.append((prices_results[predict_index][1], prices_results[predict_index + inputdays - 1][1], symbol_results[0]))

            mycursor.executemany(update_sql, update_val)

            mydb.commit()    # 数据表内容有更新，必须使用到该语句
        print(str(datetime.datetime.now()) + "lines=" + str(symbol_index) + "symbol=" + symbol_results[0])
        if symbol_index >= predict_batch_size:
            break
        #print(price_list)
    price_file.close()
    os.rename(price_filename_txt, price_filename_csv)

    return symbol_id_list
