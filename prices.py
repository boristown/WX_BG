# -*- coding: utf-8 -*-
# filename: prices.py
import mysql.connector
import mypsw
import os
import re
import requests
import datetime
import time

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
    select_alias_statment = "SELECT DISTINCT symbol, MARKET_TYPE FROM symbol_alias order by RAND()"
    mycursor.execute(select_alias_statment)
    try:
        alias_results = mycursor.fetchall()
    except:
        print("数据库连接失败！")
        return None
    if len(alias_results) == 0:
        ret_message = "市场表symbol_alias无数据！请先维护！" 
        return ret_message
    
    startdays = 300
    inputdays = 120

    url = "https://cn.investing.com/instruments/HistoricalDataAjax"

    headers = {
        'accept': "text/plain, */*; q=0.01",
        'origin': "https://cn.investing.com",
        'x-requested-with': "XMLHttpRequest",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
        'content-type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache",
        'postman-token': "17db1643-3ef6-fa9e-157b-9d5058f391e4"
        }

    st_date_str = (datetime.datetime.utcnow() + datetime.timedelta(days = -startdays)).strftime("%Y-%m-%d").replace("-","%2F")
    end_date_str = (datetime.datetime.utcnow()).strftime("%Y-%m-%d").replace("-","%2F")
    
    symbol_index = 0
    time_text =  datetime.datetime.utcnow().strftime("%Y%m%d")
    price_filename_txt = os.path.join('Output/prices/part-WX_' + time_text + '.txt')
    price_filename_csv = os.path.join('Output/prices/price-WX_' + time_text + '.csv')
    price_file = open(price_filename_txt, "w", encoding="utf-8")
    price_file.truncate()
    symbol_id_list = []
    for alias_result in alias_results:
        if alias_result[1] == '外汇':
            smlID_str = str(int(alias_result[0]) + 106681)
        else:
            smlID_str = '25609848'
            
        payload = "action=historical_data&curr_id="+ alias_result[0] +"&end_date=" + end_date_str + "&header=null&interval_sec=Daily&smlID=" + smlID_str + "&sort_col=date&sort_ord=DESC&st_date=" + st_date_str
        
        response = None
        #for response_index in range(2):
        try:
            #time.sleep(0.3)
            response = requests.request("POST", url, data=payload, headers=headers, verify=False, timeout=40)
            #break
        except:
            break
            print("Retry after 7 seconds……")
            time.sleep(7)
        if response == None:
            continue
        table_pattern = r'<tr>.+?<td.+?data-real-value="([^><"]+?)".+?</td>' \
            '.+?data-real-value="([^><"]+?)".+?</td>.+?data-real-value="([^><"]+?)".+?</td>'  \
            '.+?data-real-value="([^><"]+?)".+?</td>.+?data-real-value="([^><"]+?)".+?</td>'  \
            '.+?data-real-value="([^><"]+?)".+?</td>'
        row_matchs = re.finditer(table_pattern,response.text,re.S)
        price_list = []
        price_count = 0
        insert_val = []
        for cell_matchs in row_matchs:
            price_count += 1
            if price_count > inputdays:
                break
            price = float(str(cell_matchs.group(2)).replace(",",""))
            #if price_count == 1 or price != price_list[price_count-2]:
            price_list.append(price)
            insert_val.append((alias_result[0], inputdays - price_count + 1, price))
            #else:
            #    price_count -= 1
        if len(price_list) != inputdays:
            print(len(price_list))
            continue
        insert_val.reverse()

        insert_sql = "INSERT INTO prices ("  \
            "SYMBOL, DAY_INDEX, PRICE" \
            ") VALUES (" \
            "%s, %s, %s) " \
            "ON DUPLICATE KEY UPDATE DAY_INDEX=VALUES(DAY_INDEX),PRICE=VALUES(PRICE)"
        try:
            mycursor.executemany(insert_sql, insert_val)

            mydb.commit()    # 数据表内容有更新，必须使用到该语句
        except:
            return
        symbol_id_list.append(alias_result[0])
        print(price_list)
        max_price = max(price_list)
        min_price = min(price_list)
        center_price = (max_price + min_price) / 2
        range_price = max_price - min_price
        if range_price <= 0:
            continue
        symbol_index+=1
        if symbol_index > 1:
            price_file.write("\n")
        price_line = ""
        print( "%d\t%s" % (symbol_index, alias_result[0])  )
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
