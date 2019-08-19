# -*- coding: utf-8 -*-
# filename: WX_BG.py
import prices
import glob
import prediction
import os

#预测数据文件
predict_file_pattern = "Output/predict/predict-*.csv"

#删除旧的预测数据
predict_files  = glob.glob(predict_file_pattern)
for predict_file in predict_files:
    os.remove(predict_file)

#读取价格并生成输入数据
symbol_id_list = prices.read_prices()

# 预测并读取结果 
while True:
    time.sleep(10)
    predict_files  = glob.glob(predict_file_pattern)
    if len(price_files) == 0:
        continue
    prediction.get_prediction(symbol_id_list, price_files[0])
