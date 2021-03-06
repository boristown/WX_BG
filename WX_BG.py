# -*- coding: utf-8 -*-
# filename: WX_BG.py
import prices
import glob
import prediction
import os
import time
import random

#预测数据文件
prices_file_pattern = "Output\\prices\\*.csv"
#预测数据文件
predict_file_pattern = "Output\\predict\\*.csv"
#预测数据文件
prices_file_second_pattern = "Output\\prices_second\\*.csv"
#预测数据文件
predict_file_second_pattern = "Output\\predict_second\\*.csv"

modeStr = {0: "v1", 1:"v2"}

predict_batch_size = 10000

while True:
    '''
    randint = random.randint(0, 9)
    if randint == 0:
        modeType = 0
    else:
        modeType = 1
    '''
    modeType = 1
    print( "mode = " + modeStr[modeType] )

    #删除旧的价格数据
    prices_files  = glob.glob(prices_file_pattern)
    for prices_file in prices_files:
        os.remove(prices_file)

    prices_files_second  = glob.glob(prices_file_second_pattern)
    for prices_file_second in prices_files_second:
        os.remove(prices_file_second)

    #删除旧的预测数据
    predict_files  = glob.glob(predict_file_pattern)
    for predict_file in predict_files:
        os.remove(predict_file)
        
    predict_files_second  = glob.glob(predict_file_second_pattern)
    for predict_file_second in predict_files_second:
        os.remove(predict_file_second)

    time.sleep(10)
    print("正在读取价格……")
    #读取价格并生成输入数据
    if modeType == 0:
        symbol_id_list = prices.read_prices()
    else:
        symbol_id_list = prices.read_pricehistory(predict_batch_size)
    try:
        if len(symbol_id_list) == 0:
            continue
    except:
        continue
    print("正在执行预测……")
    # 预测并读取结果 
    while True:
        time.sleep(1)
        predict_files  = glob.glob(predict_file_pattern)
        predict_files_second  = glob.glob(predict_file_second_pattern)
        if len(predict_files) == 0 or len(predict_files_second) == 0:
            continue
        print("检测到预测文件：", predict_files[0])
        print("检测到预测文件2：", predict_files_second[0])
        time.sleep(2)
        if modeType == 0:
            prediction.get_prediction(symbol_id_list, predict_files[0])
        else:
            prediction.get_predictionhistory(symbol_id_list, predict_files[0], predict_files_second[0])
        break
    print("预测执行完毕！")
    time.sleep(20)
