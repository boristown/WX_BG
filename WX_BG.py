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

    url = "https://www.investing.com/instruments/HistoricalDataAjax"

    headers = {
        'accept': "text/plain, */*; q=0.01",
        'origin': "https://www.investing.com",
        'x-requested-with': "XMLHttpRequest",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
        'content-type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache",
        'postman-token': "17db1643-3ef6-fa9e-157b-9d5058f391e4"
        }

    st_date_str = (datetime.datetime.utcnow() + datetime.timedelta(days = -startdays)).strftime("%m-%d-%Y").replace("-","%2F")
    end_date_str = (datetime.datetime.utcnow()).strftime("%m-%d-%Y").replace("-","%2F")
    
    time.sleep(0.5)
    
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
        print(price_list)
    return 'success'

print(bg_update())