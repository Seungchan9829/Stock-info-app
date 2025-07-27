from flask import Flask, render_template, request
from filter_low_di20 import filter_low_di20
import os
from datetime import date
from pathlib import Path
import pandas as pd


app = Flask(__name__)

LOW_DI20_DIR = Path(__file__).parent / "low_di20"

@app.before_request
def load_low_di20():
    global low_di20
    low_di20 = filter_low_di20()



@app.route('/', methods = ['GET'])
def home():

    selected_date = request.args.get(
        'date',
        date.today().strftime('%Y-%m-%d')
    )

    # pkl 파일 경로
    pkl_file = LOW_DI20_DIR / f"{selected_date}_stock_list.pkl"

    if pkl_file.exists():
        low_di20 = pd.read_pickle(pkl_file)
    else:
        low_di20 = []

    return render_template('index.html', low_di20 = low_di20, selected_date = selected_date)

if __name__ == '__main__':

    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5001)),
        debug=False
    )

    # 메인에서 필터함수 실행되고 low_di20 따로 저장 -> low_di20을 메모리로도 할수있긴함