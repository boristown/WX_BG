# -*- coding: utf-8 -*-
# filename: WX_BG.py
import prices
import glob
import prediction
import os
import time

#预测数据文件
prices_file_pattern = "Output\\prices\\*.csv"
#预测数据文件
predict_file_pattern = "Output\\predict\\*.csv"

#删除旧的价格数据
prices_files  = glob.glob(prices_file_pattern)
for prices_file in prices_files:
    os.remove(prices_file)

#删除旧的预测数据
predict_files  = glob.glob(predict_file_pattern)
for predict_file in predict_files:
    os.remove(predict_file)

time.sleep(3)
#读取价格并生成输入数据
symbol_id_list = prices.read_prices()

print("正在执行预测……")
# 预测并读取结果 
while True:
    time.sleep(1)
    predict_files  = glob.glob(predict_file_pattern)
    if len(predict_files) == 0:
        continue
    print("检测到预测文件：", predict_files[0])
    time.sleep(3)
    prediction.get_prediction(symbol_id_list, predict_files[0])
    break
