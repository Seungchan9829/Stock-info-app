from flask import Flask, render_template
from filter_low_di20 import filter_low_di20
import os

app = Flask(__name__)

@app.route('/')
def home():
    # 
    low_di20 = filter_low_di20()

    return render_template('index.html', low_di20 = low_di20)

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5000)),
        debug=True
    )