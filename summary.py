import pandas as pd
from datetime import datetime
import os, sys
import traceback

sys.path.append(os.getcwd())


def calculate_stats_from_trades(trades, starting_capital):
    try:
        stats = {}
        current_drawdown = 0
        max_drawdown = 0
        cumulative_pnl = 0
        peak_pnl = 0

        max_capital_deployment = max(trades["Entry Price"])
        totalCost = trades["Cost per Lot"].sum()
        capital_deployment_total = max(trades["Entry Price"]) * len(trades) + totalCost

        for pnl in trades["Net PnL per Lot"]:
            cumulative_pnl += pnl
            if pnl > 0:
                peak_pnl = cumulative_pnl
                current_drawdown = 0
            else:
                current_drawdown = peak_pnl - cumulative_pnl
                max_drawdown = max(max_drawdown, current_drawdown)

        if "Entry Timestamp" in trades:
            profitable_days = trades.groupby(trades["Entry Timestamp"].dt.date)[
                "Net PnL per Lot"
            ].sum()
            percent_profitable_days = (
                len(profitable_days[profitable_days > 0]) / len(profitable_days)
                if len(profitable_days) > 0
                else 0
            )
        else:
            percent_profitable_days = 0

        total_negative_pnl = abs(
            trades[trades["Net PnL per Lot"] < 0]["Net PnL per Lot"].sum()
        )
        profit_factor = (
            trades[trades["Net PnL per Lot"] > 0]["Net PnL per Lot"].sum()
            / total_negative_pnl
            if total_negative_pnl > 0
            else 0
        )

        if "Hold Time" in trades:
            avg_duration = trades["Hold Time"].mean()
        else:
            avg_duration = 0

        expiry_day_pnl = 0
        totalNumberOfExpiryTrades = 0
        expiry_day_trades = trades[trades["Expiry Day Flag"] == True]

        if not expiry_day_trades.empty:
            totalNumberOfExpiryTrades = len(expiry_day_trades)
            expiry_day_pnl = expiry_day_trades["Net PnL per Lot"].sum()
            expiry_day_win_ratio = len(
                expiry_day_trades[expiry_day_trades["Net PnL per Lot"] > 0]
            ) / len(expiry_day_trades)
        else:
            expiry_day_win_ratio = 0

        trades_between_0920_1445 = trades[
            (
                trades["Entry Timestamp"].dt.time
                >= datetime.strptime("09:20:00", "%H:%M:%S").time()
            )
            & (
                trades["Entry Timestamp"].dt.time
                < datetime.strptime("14:45:00", "%H:%M:%S").time()
            )
        ]

        trades_between_1445_1530 = trades[
            (
                trades["Entry Timestamp"].dt.time
                >= datetime.strptime("14:45:00", "%H:%M:%S").time()
            )
            & (
                trades["Entry Timestamp"].dt.time
                <= datetime.strptime("15:30:00", "%H:%M:%S").time()
            )
        ]

        if not trades_between_0920_1445.empty:
            totalNumberOfTrades_0920_1445 = len(trades_between_0920_1445)
            pnl_0920_1445 = trades_between_0920_1445["Net PnL per Lot"].sum()
            win_ratio_0920_1445 = len(
                trades_between_0920_1445[
                    trades_between_0920_1445["Net PnL per Lot"] > 0
                ]
            ) / len(trades_between_0920_1445)
        else:
            totalNumberOfTrades_0920_1445 = 0
            pnl_0920_1445 = 0
            win_ratio_0920_1445 = 0

        if not trades_between_1445_1530.empty:
            totalNumberOfTrades_1445_1530 = len(trades_between_1445_1530)
            pnl_1445_1530 = trades_between_1445_1530["Net PnL per Lot"].sum()
            win_ratio_1445_1530 = len(
                trades_between_1445_1530[
                    trades_between_1445_1530["Net PnL per Lot"] > 0
                ]
            ) / len(trades_between_1445_1530)
        else:
            totalNumberOfTrades_1445_1530 = 0
            pnl_1445_1530 = 0
            win_ratio_1445_1530 = 0

        drawDownDaily_pnl = trades.groupby(trades["Entry Timestamp"].dt.date)[
            "Net PnL per Lot"
        ].sum()
        drawDownCumulative_pnl = drawDownDaily_pnl.cumsum()
        drawDownPeak_pnl = drawDownCumulative_pnl.cummax()
        DayWiseDrawdown = drawDownPeak_pnl - drawDownCumulative_pnl
        daywise_max_drawdown = DayWiseDrawdown.max()

        trades["Month"] = trades["Entry Timestamp"].dt.to_period("M")
        monthly_pnl = trades.groupby("Month")["Net PnL per Lot"].sum()

        monthly_pnl_dict = monthly_pnl.to_dict()

        stats.update(
            {
                "Max Capital Deployment": max_capital_deployment,
                "Total Capital Deployment": capital_deployment_total,
                "Max Drawdown on Capital": (max_drawdown / max_capital_deployment),
                "Return On Capital": (
                    (trades["Net PnL per Lot"].sum()) / max_capital_deployment
                ),
                "Total Cost": totalCost,
                "PnL": trades["PnL per Lot"].sum(),
                "Net PnL": trades["Net PnL per Lot"].sum(),
                "Win Rate": (
                    len(trades[trades["PnL per Lot"] > 0]) / len(trades)
                    if len(trades) > 0
                    else 0
                ),
                "Profit Factor": profit_factor,
                "Expiry Day PnL": expiry_day_pnl,
                "Average Return per Trade": (trades["Net PnL per Lot"].mean()),
                "Total Trades": len(trades),
                "Profitable Trades": len(trades[trades["Net PnL per Lot"] > 0]),
                "Losing Trades": len(trades[trades["Net PnL per Lot"] < 0]),
                "Average Duration": avg_duration,
                "Average Winning Trade": (
                    trades[trades["Net PnL per Lot"] > 0]["Net PnL per Lot"].mean()
                    if len(trades[trades["Net PnL per Lot"] > 0]) > 0
                    else 0
                ),
                "Average Losing Trade": (
                    trades[trades["Net PnL per Lot"] < 0]["Net PnL per Lot"].mean()
                    if len(trades[trades["Net PnL per Lot"] < 0]) > 0
                    else 0
                ),
                "Largest Winning Trade": (
                    trades["Net PnL per Lot"].max() if len(trades) > 0 else 0
                ),
                "Largest Losing Trade": (
                    trades["Net PnL per Lot"].min() if len(trades) > 0 else 0
                ),
                "Risk Reward Ratio": (
                    abs(trades[trades["Net PnL per Lot"] > 0]["Net PnL per Lot"].mean())
                    / abs(
                        trades[trades["Net PnL per Lot"] < 0]["Net PnL per Lot"].mean()
                    )
                    if len(trades[trades["Net PnL per Lot"] < 0]) > 0
                    else "N/A"
                ),
                "CAGR": (
                    (
                        (
                            (trades["Net PnL per Lot"].sum() + starting_capital)
                            / starting_capital
                        )
                        - 1
                    )
                    if starting_capital > 0
                    else "N/A"
                ),
                "Calmar Ratio": (
                    (
                        (
                            (trades["Net PnL per Lot"].sum() + starting_capital)
                            / starting_capital
                        )
                        - 1
                    )
                    / max_drawdown
                    if max_drawdown > 0
                    else "N/A"
                ),
                "Consecutive Wins": (
                    trades["Net PnL per Lot"]
                    .gt(0)
                    .astype(int)
                    .groupby(trades["Net PnL per Lot"].lt(0).cumsum())
                    .sum()
                    .max()
                    if len(trades) > 0
                    else 0
                ),
                "Consecutive Losses": (
                    trades["Net PnL per Lot"]
                    .lt(0)
                    .astype(int)
                    .groupby(trades["Net PnL per Lot"].gt(0).cumsum())
                    .sum()
                    .max()
                    if len(trades) > 0
                    else 0
                ),
                "Percent Profitable Days": percent_profitable_days,
                "Best Day PnL": (
                    profitable_days.max() if len(profitable_days) > 0 else 0
                ),
                "Worst Day PnL": (
                    profitable_days.min() if len(profitable_days) > 0 else 0
                ),
                "Max Drawdown": max_drawdown,
                "Day-wise PnL Drawdown": daywise_max_drawdown,
                "Expiry Day Net Pnl": expiry_day_pnl,
                "Expiry Day Win Ratio": expiry_day_win_ratio,
                "TotalNumberOfExpiryTrades": totalNumberOfExpiryTrades,
                "9:20 to 14:45 Net PnL": pnl_0920_1445,
                "9:20 to 14:45 Win Ratio": win_ratio_0920_1445,
                "9:20 to 14:45 Trades": totalNumberOfTrades_0920_1445,
                "14:45 to 15:30 Net PnL": pnl_1445_1530,
                "14:45 to 15:30 Win Ratio": win_ratio_1445_1530,
                "14:45 to 15:30 Trades": totalNumberOfTrades_1445_1530,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

        for month, pnl in monthly_pnl_dict.items():
            stats[f"PnL_{month.strftime('%b-%Y')}"] = pnl

        return stats
    except Exception as e:
        print("Error in calculating stats from trades:", e)
        traceback.print_exc()


def generate_markdown_report(trades, template_path, output_path, starting_capital):
    try:
        with open(template_path, "r") as file:
            template = file.read()

        if isinstance(trades, list):
            trades = pd.DataFrame(trades)
        elif not isinstance(trades, pd.DataFrame):
            raise ValueError(
                "The 'trades' argument must be a list of dictionaries or a pandas DataFrame."
            )

        stats = {
            "Trade Month": "Unknown",
            "Max Capital Deployment": 0,
            "Max Drawdown on Capital": 0,
            "Return On Capital": 0,
            "Total Cost": 0,
            "PnL": 0,
            "Net PnL": 0,
            "Win Rate": 0,
            "Profit Factor": 0,
            "Expiry Day PnL": 0,
            "Average Return per Trade": 0,
            "Total Trades": 0,
            "Profitable Trades": 0,
            "Losing Trades": 0,
            "Average Duration": 0,
            "Average Winning Trade": 0,
            "Average Losing Trade": 0,
            "Largest Winning Trade": 0,
            "Largest Losing Trade": 0,
            "Risk Reward Ratio": 0,
            "CAGR": 0,
            "Calmar Ratio": 0,
            "Consecutive Wins": 0,
            "Consecutive Losses": 0,
            "Max Drawdown": 0,
            "Percent Profitable Days": 0,
            "Best Day PnL": 0,
            "Worst Day PnL": 0,
            "Expiry Day Net Pnl": 0,
            "Expiry Day Win Ratio": 0,
            "TotalNumberOfExpiryTrades": 0,
            "9:20 to 14:45 Net PnL": 0,
            "9:20 to 14:45 Win Ratio": 0,
            "9:20 to 14:45 Trades": 0,
            "14:45 to 15:30 Net PnL": 0,
            "14:45 to 15:30 Win Ratio": 0,
            "14:45 to 15:30 Trades": 0,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        if not trades.empty:
            stats.update(calculate_stats_from_trades(trades, starting_capital))

        try:
            report = template.format(**stats)
        except KeyError as e:
            print(f"Error in formatting the template. Missing key: {e}")
            raise
        except IndexError as e:
            print(f"Error in formatting the template. Positional index issue: {e}")
            raise

        with open(output_path, "w") as f:
            f.write(report)

        print(f"Markdown report saved to {output_path}")
        return stats
    except Exception as e:
        print("Error in generating Markdown report:", e)
        traceback.print_exc()
