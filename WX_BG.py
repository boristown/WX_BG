# -*- coding: utf-8 -*- 
# filename: WX_BG.py 
import mysql.connector
import mypsw

print(bg_update())

def bg_update():
    output_text = ''   
    mydb = mysql.connector.connect( 
        host=mypsw.wechatadmin.host, 
        user=mypsw.wechatadmin.user, 
        passwd=mypsw.wechatadmin.passwd, 
        database=mypsw.wechatadmin.database, 
        auth_plugin='mysql_native_password' )
    mycursor = mydb.cursor()
    select_alias_statment = "SELECT DISTINCT symbol FROM symbol_alias"
    mycursor.execute(select_alias_statment)
    alias_results = mycursor.fetchall()
    if len(alias_results) == 0:
        ret_message = "市场表symbol_alias无数据！请先维护！" 
        return ret_message
    for alias_result in alias_results:
        print(alias_result[0])
    return 'success'