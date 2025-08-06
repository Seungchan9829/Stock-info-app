from flask import Flask, render_template, request
from filter_low_di20 import filter_low_di20
import os
from datetime import date
from pathlib import Path
import pandas as pd
from service.chart_service import get_credit_finance_data_by_period, get_kospi_price_data_by_period, get_credit_data_di20

app = Flask(__name__)

LOW_DI20_DIR = Path(__file__).parent / "low_di20"

MENU_ITEMS = [
    ("이격도", "home"),
    ("신용잔고", "credit_balance"),
    ("옵션분석", "options"),
    ("백테스팅", "backtesting"),
]

@app.context_processor
def inject_globals():
    return {
        "menu_items": MENU_ITEMS,
        # 템플릿에서 selected_date 사용 가능
        "selected_date": request.args.get(
            "date",
            date.today().strftime('%Y-%m-%d')
        )
    }

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


@app.route('/credit_balance')
def credit_balance():
    # 추후 로직 추가: 신용잔고 데이터 계산/조회
    credit_data = []  # TODO: 실제 데이터 로드
    return render_template('credit_balance.html', credit_data=credit_data)

@app.route('/options')
def options():
    # 옵션 분석 화면 구현
    return render_template('options.html')

@app.route('/backtesting')
def backtesting():
    # 백테스팅 화면 구현
    return render_template('backtesting.html')

@app.route('/credit-status')
def credit_status():
    period = request.args.get("period", "1y")
    # 기간별 신용잔고
    credit_raw_data = get_credit_finance_data_by_period(period)
    credit_raw_data.sort(key = lambda row: row[0])
    # 신용잔고 이격도, p7 가져오기
    credit_di20, p7 = get_credit_data_di20(period)
    sorted_credit_di20 = credit_di20.sort_values(by='date', ascending=True)

    credit_di20_data = [
        {
            "date": row["date"].strftime('%Y-%m-%d'),   # 컬럼명으로 접근
            "di20" : row["DI20"]
        }
        for _, row in sorted_credit_di20.iterrows()
    ]


    # ✅ 날짜 문자열로 변환
    credit_data = [
        {
            "date": row[0].strftime('%Y-%m-%d'),
            "loan_total": row[1],
            "loan_kospi": row[2],
            "loan_kosdaq": row[3]
        }
        for row in credit_raw_data
    ]

    # 기간별 코스피 가격
    kospi_price_raw_data = get_kospi_price_data_by_period(period)
    kospi_price_raw_data.sort(key = lambda row : row[0])

    kospi_data = [
        {
            "date": row[0].strftime('%Y-%m-%d'),
            "kospi_index" : row[1]

        } for row in kospi_price_raw_data
    ]
    return render_template('pages/credit_status.html', credit_data=credit_data, kospi_index = kospi_data, period = period, credit_di20_data = credit_di20_data, p7 = p7 )


if __name__ == '__main__':

    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5001)),
        debug=False
    )

