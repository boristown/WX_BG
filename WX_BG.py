# -*- coding: utf-8 -*-
# filename: WX_BG.py
import mysql.connector
import mypsw
import os
import re
import requests
import datetime
import time

def bg_update():
    output_text = '' 
    mydb = mysql.connector.connect(host=mypsw.wechatadmin.host, 
        user=mypsw.wechatadmin.user, 
        passwd=mypsw.wechatadmin.passwd, 
        database=mypsw.wechatadmin.database, 
        auth_plugin='mysql_native_password')
    mycursor = mydb.cursor()
    select_alias_statment = "SELECT DISTINCT symbol FROM symbol_alias"
    mycursor.execute(select_alias_statment)
    alias_results = mycursor.fetchall()
    if len(alias_results) == 0:
        ret_message = "市场表symbol_alias无数据！请先维护！" 
        return ret_message
    
    startdays = 200
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
    
    time.sleep(0.5)
    symbol_index = 0
    #list_filename = os.path.join('Output/WX/list-' + markets[mode] + '.txt')
    price_filename = os.path.join('Output/WX/price-WX_' + datetime.datetime.utcnow().strftime("%Y%m%d") + '.csv')
    reversed_filename = os.path.join('Output/WX/reversed-WX_' + datetime.datetime.utcnow().strftime("%Y%m%d") + '.csv')
    #list_file = open(list_filename, "w", encoding="utf-8")
    price_file = open(price_filename, "w", encoding="utf-8")
    reversed_file = open(reversed_filename, "w", encoding="utf-8")
    #list_file.truncate()
    price_file.truncate()
    reversed_file.truncate()
    for alias_result in alias_results:
        payload = "action=historical_data&curr_id="+ alias_result[0] +"&end_date=" + end_date_str + "&header=null&interval_sec=Daily&smlID=&sort_col=date&sort_ord=DESC&st_date=" + st_date_str
        while True:
            try:
                response = requests.request("POST", url, data=payload, headers=headers, verify=False)
                break
            except:
                print("Retry after 7 seconds……")
                time.sleep(3)
        table_pattern = r'<tr>.+?<td.+?data-real-value="([^><"]+?)".+?</td>' \
            '.+?data-real-value="([^><"]+?)".+?</td>.+?data-real-value="([^><"]+?)".+?</td>'  \
            '.+?data-real-value="([^><"]+?)".+?</td>.+?data-real-value="([^><"]+?)".+?</td>'  \
            '.+?data-real-value="([^><"]+?)".+?</td>'
        row_matchs = re.finditer(table_pattern,response.text,re.S)
        price_list = []
        price_count = 0
        for cell_matchs in row_matchs:
            price_count += 1
            if price_count > inputdays:
                break
            price = float(str(cell_matchs.group(2)).replace(",",""))
            if price_count == 1 or price != price_list[price_count-2]:
                price_list.append(price)
            else:
                price_count -= 1
        if len(price_list) != inputdays:
            print(len(price_list))
            continue
        max_price = max(price_list)
        min_price = min(price_list)
        center_price = (max_price + min_price) / 2
        range_price = max_price - min_price
        if range_price <= 0:
            continue
        symbol_index+=1
        if symbol_index > 1:
            #list_file.write("\n")
            price_file.write("\n")
            reversed_file.write("\n")
        #list_file.write(marketName + "\t" + symbol_str)
        price_line = ""
        reversed_line = ""
        print( "%d\t%s\t%s" % (symbol_index, curr_id_str, symbol_str)  )
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
                reversed_line = "," + reversed_line
            price_line += str(price_list[i])
            reversed_line = str(price_list[i]) + reversed_line
        price_file.write(price_line)
        reversed_file.write(reversed_line)
        #print(price_line)
        print(price_list)
    #list_file.close()
    price_file.close()
    reversed_file.close()
    return 'success'

print(bg_update())