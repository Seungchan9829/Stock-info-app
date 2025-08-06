def format_low_di20_stocks(stocks):
    lines = []
    lines.append("티커   | 현재 DI20   | 분위수 DI20")
    lines.append("-" * 35)
    for ticker, curr, quant in stocks:
        # float64 → float 변환 및 소수점 2자리 포맷
        lines.append(f"{ticker:<6} | {float(curr):>10.2f} | {float(quant):>11.2f}")
    return "\n".join(lines)