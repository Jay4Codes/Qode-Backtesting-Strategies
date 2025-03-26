import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from datetime import time
import sys
import os
import base64

sys.path.append(os.getcwd())
from summary import calculate_stats_from_trades


def get_binary_file_downloader_html(bin_file, file_label="File"):
    with open(bin_file, "rb") as f:
        data = f.read()

    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:file/txt;base64,{bin_str}" download="{os.path.basename(bin_file)}">{file_label}</a>'
    return href


def load_trades_file(uploaded_file):
    try:
        if uploaded_file.name.endswith(".csv"):
            trades_df = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith(".xlsx"):
            trades_df = pd.read_excel(uploaded_file)
        else:
            st.error("Unsupported file type. Please upload CSV or Excel.")
            return None

        timestamp_columns = ["Entry Timestamp", "Exit Timestamp"]
        for col in timestamp_columns:
            if col in trades_df.columns:
                trades_df[col] = pd.to_datetime(trades_df[col])

        trades_df = trades_df[
            trades_df["Entry Timestamp"].dt.date != pd.Timestamp("2024-06-04").date()
        ]

        trades_df = trades_df[
            trades_df["Entry Timestamp"].dt.date != pd.Timestamp("2024-04-18").date()
        ]

        additional_columns = [
            "Instruments",
            "Days to Expiry",
            "Expiry Day Flag",
            "Month",
            "Hold Time",
            "Exit Reason",
        ]
        for col in additional_columns:
            if col not in trades_df.columns:
                st.warning(f"Column '{col}' not found in the uploaded file.")

        return trades_df
    except Exception as e:
        st.error(f"Error loading file: {e}")
        return None


def apply_filters(trades_df):
    st.sidebar.markdown("## 🔍 Advanced Filters")
    st.sidebar.markdown("---")

    quantity_multiplier = st.sidebar.number_input(
        "Quantity Multiplier",
        min_value=1.0,
        value=1.0,
        step=0.5,
        help="Multiply the PnL and other metrics by this factor",
    )

    st.sidebar.markdown("### 📅 Date and Time Filters")

    start_time = st.sidebar.time_input(
        "Start Time", value=time(9, 15), help="Filter trades starting from this time"
    )
    end_time = st.sidebar.time_input(
        "End Time", value=time(15, 30), help="Filter trades ending before this time"
    )

    trades_df["Entry Time"] = trades_df["Entry Timestamp"].dt.time
    trades_df = trades_df[
        (trades_df["Entry Time"] >= start_time) & (trades_df["Entry Time"] <= end_time)
    ]

    st.sidebar.markdown("### 📊 Additional Filters")

    if "Days to Expiry" in trades_df.columns:
        min_days = int(trades_df["Days to Expiry"].min())
        max_days = int(trades_df["Days to Expiry"].max())
        days_range = st.sidebar.slider(
            "Days to Expiry",
            min_value=min_days,
            max_value=max_days,
            value=(min_days, max_days),
        )
        trades_df = trades_df[
            (trades_df["Days to Expiry"] >= days_range[0])
            & (trades_df["Days to Expiry"] <= days_range[1])
        ]

    filter_columns = [
        "Expiry Day Flag",
        "Month",
        "Hold Time",
        "Exit Reason",
        "Instruments",
    ]

    for col in filter_columns:
        if col in trades_df.columns:
            if col == "Expiry Day Flag":
                filter_options = st.sidebar.multiselect(
                    "Expiry Day Trades", [True, False], default=[True, False]
                )
                trades_df = trades_df[trades_df[col].isin(filter_options)]
            elif col == "Month":
                month_filter = st.sidebar.multiselect(
                    "Select Months",
                    sorted(trades_df[col].unique()),
                    default=sorted(trades_df[col].unique()),
                )
                trades_df = trades_df[trades_df[col].isin(month_filter)]
            elif col == "Hold Time":
                min_hold, max_hold = (
                    trades_df["Hold Time"].min(),
                    trades_df["Hold Time"].max(),
                )
                hold_time_range = st.sidebar.slider(
                    "Hold Time (minutes)",
                    min_value=float(min_hold),
                    max_value=float(max_hold),
                    value=(float(min_hold), float(max_hold)),
                )
                trades_df = trades_df[
                    (trades_df["Hold Time"] >= hold_time_range[0])
                    & (trades_df["Hold Time"] <= hold_time_range[1])
                ]
            elif col == "Exit Reason":
                exit_reason_filter = st.sidebar.multiselect(
                    "Exit Reasons",
                    trades_df[col].unique(),
                    default=trades_df[col].unique(),
                )
                trades_df = trades_df[trades_df[col].isin(exit_reason_filter)]

    pnl_range = st.sidebar.slider(
        "Net PnL per Lot Range",
        min_value=float(trades_df["Net PnL per Lot"].min()),
        max_value=float(trades_df["Net PnL per Lot"].max()),
        value=(
            float(trades_df["Net PnL per Lot"].min()),
            float(trades_df["Net PnL per Lot"].max()),
        ),
    )
    trades_df = trades_df[
        (trades_df["Net PnL per Lot"] >= pnl_range[0])
        & (trades_df["Net PnL per Lot"] <= pnl_range[1])
    ]

    numeric_columns = [
        "PnL per Lot",
        "Net PnL per Lot",
        "Cost per Lot",
        "Entry Price",
        "Exit Price",
    ]
    for col in numeric_columns:
        if col in trades_df.columns:
            trades_df[col] *= quantity_multiplier

    if st.sidebar.button("Download Filtered Trades"):
        trades_df.to_csv("filtered_trades.csv", index=False)
        st.markdown(
            get_binary_file_downloader_html("filtered_trades.csv", "Filtered Trades"),
            unsafe_allow_html=True,
        )

    return trades_df


def instrument_analysis(trades_df):
    st.header("🔬 Strategy Performance Analysis")

    tabs = st.tabs(
        [
            "General Plots",
            "Detailed Metrics",
            "Net PnL vs Days to Expiry",
        ]
    )

    with tabs[0]:
        create_advanced_visualizations(trades_df)

    with tabs[1]:
        stats = calculate_stats_from_trades(trades_df)

        st.subheader("Strategy Overview Metrics")

        col1, col2 = st.columns(2)

        with col1:
            st.metric("Net PnL", f"₹{stats['Net PnL']:,.2f}")
            st.metric("PnL", f"₹{stats['PnL']:,.2f}")
            st.metric("Win Rate", f"{stats['Win Rate']*100:.2f}%")
            st.metric("Profit Factor", f"{stats['Profit Factor']:.2f}")
            st.metric(
                "Average Return per Trade", f"₹{stats['Average Return per Trade']:,.2f}"
            )
            st.metric("Calmar Ratio", f"{stats['Calmar Ratio']:.2f}")
            st.metric("CAGR", f"{stats['CAGR']:.2f}")
            st.metric("Total Cost", f"₹{stats['Total Cost']:,.2f}")
            st.metric("Average Duration", f"{stats['Average Duration']:,.2f} mins")
            st.metric(
                "Average Winning Trade", f"₹{stats['Average Winning Trade']:,.2f}"
            )
            st.metric("Average Losing Trade", f"₹{stats['Average Losing Trade']:,.2f}")
            st.metric(
                "Percent Profitable Days",
                f"{stats['Percent Profitable Days']*100:.2f}%",
            )
            st.metric("Best Day PnL", f"₹{stats['Best Day PnL']:,.2f}")
            st.metric("Worst Day PnL", f"₹{stats['Worst Day PnL']:,.2f}")
            st.metric("Max Drawdown", f"{stats['Max Drawdown']:.2f}%")
            st.metric("Expiry Day Net Pnl", f"₹{stats['Expiry Day Net Pnl']:,.2f}")

        with col2:
            st.metric("Total Trades", stats["Total Trades"])
            st.metric("Consecutive Wins", stats["Consecutive Wins"])
            st.metric("Consecutive Losses", stats["Consecutive Losses"])
            st.metric("Risk Reward Ratio", f"{stats['Risk Reward Ratio']:.2f}")
            st.metric("Return on Capital", f"{stats['Return On Capital']:.2f}%")
            st.metric("Max Capital Required", f"₹{stats['Max Capital Required']:,.2f}")
            st.metric(
                "Total Capital Deployment", f"₹{stats['Total Capital Deployment']:,.2f}"
            )
            st.metric(
                "Largest Winning Trade", f"₹{stats['Largest Winning Trade']:,.2f}"
            )
            st.metric("Largest Losing Trade", f"₹{stats['Largest Losing Trade']:,.2f}")
            st.metric(
                "9:20 to 14:45 Net PnL", f"₹{stats['9:20 to 14:45 Net PnL']:,.2f}"
            )
            st.metric(
                "9:20 to 14:45 Win Ratio",
                f"{stats['9:20 to 14:45 Win Ratio']*100:.2f}%",
            )
            st.metric("9:20 to 14:45 Trades", stats["9:20 to 14:45 Trades"])
            st.metric(
                "14:45 to 15:30 Net PnL", f"₹{stats['14:45 to 15:30 Net PnL']:,.2f}"
            )
            st.metric(
                "14:45 to 15:30 Win Ratio",
                f"{stats['14:45 to 15:30 Win Ratio']*100:.2f}%",
            )
            st.metric("14:45 to 15:30 Trades", stats["14:45 to 15:30 Trades"])
            st.metric(
                "Expiry Day Win Ratio", f"{stats['Expiry Day Win Ratio']*100:.2f}%"
            )
            st.metric(
                "Total Number of Expiry Trades", stats["TotalNumberOfExpiryTrades"]
            )

        fig_pie = px.pie(
            values=[stats["Profitable Trades"], stats["Losing Trades"]],
            names=["Profitable Trades", "Losing Trades"],
            title="Profitable vs Losing Trades",
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )

        st.plotly_chart(fig_pie, use_container_width=True)

        st.write(
            "This pie chart shows the distribution of profitable and losing trades. A higher proportion of profitable trades indicates a successful trading strategy."
        )

    with tabs[2]:
        if "Days to Expiry" in trades_df.columns:
            fig_scatter = px.scatter(
                trades_df,
                x="Days to Expiry",
                y="Net PnL per Lot",
                color="Instruments" if "Instruments" in trades_df.columns else None,
                hover_data=(
                    ["Entry Timestamp", "Instruments"]
                    if "Instruments" in trades_df.columns
                    else ["Entry Timestamp"]
                ),
                title="Net PnL vs Days to Expiry",
                labels={
                    "Days to Expiry": "Days to Expiry",
                    "Net PnL per Lot": "Net PnL per Lot",
                },
                color_discrete_sequence=px.colors.qualitative.Plotly,
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
            st.write(
                "This scatter plot shows how the Net PnL varies with the number of days to expiry. Each point represents a trade, helping you understand the relationship between trade profitability and time to expiration."
            )


def equity_curve(trades_df):
    trades_df = trades_df.sort_values("Entry Timestamp")

    fig_equity_curve = go.Figure()
    fig_equity_curve.add_trace(
        go.Scatter(
            x=trades_df["Entry Timestamp"],
            y=trades_df["Cumulative Capital"],
            mode="lines",
            name="Equity Curve",
            line=dict(color="blue"),
            hovertemplate="Date: %{x}<br>Equity: ₹%{y:.2f}<extra></extra>",
        )
    )
    fig_equity_curve.update_layout(
        title="Equity Curve",
        xaxis_title="Date",
        yaxis_title="Equity (₹)",
    )
    st.plotly_chart(fig_equity_curve, use_container_width=True)
    st.write(
        "The Equity Curve shows the cumulative performance of your trading strategy over time. An upward trending line indicates consistent profitability, while flat or downward trends suggest the strategy might need refinement."
    )


def create_advanced_visualizations(filtered_trades_df):
    equity_curve(filtered_trades_df)
    drawdown_analysis(filtered_trades_df)
    trade_profit_distribution(filtered_trades_df)
    win_loss_trades_analysis(filtered_trades_df)
    monthly_pnl_trend(filtered_trades_df)


def drawdown_analysis(trades_df):
    trades_df = trades_df.sort_values("Entry Timestamp")
    trades_df["Cumulative PnL"] = trades_df["Net PnL per Lot"].cumsum()
    cumulative_max = trades_df["Cumulative PnL"].cummax()
    drawdown = trades_df["Cumulative PnL"] - cumulative_max

    fig_drawdown = go.Figure()
    fig_drawdown.add_trace(
        go.Scatter(
            x=trades_df["Entry Timestamp"],
            y=drawdown,
            mode="lines",
            name="Drawdown",
            fill="tozeroy",
            line=dict(color="red"),
            hovertemplate="Date: %{x}<br>Drawdown: %{y:.2f}<extra></extra>",
        )
    )
    fig_drawdown.update_layout(
        title="Portfolio Drawdown Analysis",
        xaxis_title="Date",
        yaxis_title="Drawdown Amount",
    )
    st.plotly_chart(fig_drawdown, use_container_width=True)
    st.write(
        "The Drawdown Analysis visualizes the largest decline in portfolio value from its peak. Lower and shorter drawdowns indicate a more stable and robust trading strategy."
    )


def trade_profit_distribution(trades_df):
    fig_histogram = px.histogram(
        trades_df,
        x="Net PnL per Lot",
        nbins=50,
        title="Distribution of Trade Profits",
        labels={"Net PnL per Lot": "Net PnL per Lot"},
        color_discrete_sequence=["blue"],
        marginal="box",
    )
    fig_histogram.update_layout(xaxis_title="Net PnL per Lot", yaxis_title="Frequency")
    st.plotly_chart(fig_histogram, use_container_width=True)
    st.write(
        "This histogram shows the distribution of trade profits. A concentration of trades around zero with a slight positive skew suggests a consistent and profitable trading strategy."
    )


def win_loss_trades_analysis(trades_df):
    win_trades = trades_df[trades_df["Net PnL per Lot"] > 0]
    loss_trades = trades_df[trades_df["Net PnL per Lot"] <= 0]

    win_loss_data = {
        "Metric": ["Win Trades", "Loss Trades"],
        "Count": [len(win_trades), len(loss_trades)],
        "Total PnL": [
            win_trades["Net PnL per Lot"].sum(),
            loss_trades["Net PnL per Lot"].sum(),
        ],
    }

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=win_loss_data["Metric"],
            y=win_loss_data["Count"],
            name="Trades Count",
            marker_color=["green", "red"],
            hovertemplate="%{y} trades<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=win_loss_data["Metric"],
            y=win_loss_data["Total PnL"],
            name="Total PnL",
            marker_color=["lightgreen", "lightcoral"],
            hovertemplate="Total PnL: ₹%{y:.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Win vs Loss Trades Analysis",
        xaxis_title="Trade Type",
        yaxis_title="Count / Total PnL",
        barmode="group",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.write(
        "This chart compares the number of winning and losing trades, along with their total PnL. A higher proportion of green bars indicates a more successful trading strategy."
    )


def monthly_pnl_trend(trades_df):
    trades_df["Month"] = trades_df["Entry Timestamp"].dt.to_period("M")
    monthly_pnl = trades_df.groupby("Month")["Net PnL per Lot"].sum().reset_index()

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=monthly_pnl["Month"].astype(str),
            y=monthly_pnl["Net PnL per Lot"],
            name="Monthly PnL",
            marker_color="blue",
            hovertemplate="Month: %{x}<br>Net PnL: ₹%{y:.2f}<extra></extra>",
        )
    )
    monthly_pnl["Cumulative PnL"] = monthly_pnl["Net PnL per Lot"].cumsum()
    fig.add_trace(
        go.Scatter(
            x=monthly_pnl["Month"].astype(str),
            y=monthly_pnl["Cumulative PnL"],
            mode="lines+markers",
            name="Cumulative PnL",
            line=dict(color="red", width=2),
            hovertemplate="Month: %{x}<br>Cumulative PnL: ₹%{y:.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Monthly PnL Trend",
        xaxis_title="Month",
        yaxis_title="PnL",
        barmode="overlay",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.write(
        "The Monthly PnL Trend displays the performance of your trading strategy on a month-by-month basis. The bar chart shows monthly profits, while the red line tracks the cumulative performance over time."
    )


def strategy_comparison_page():
    st.title("🔍 Strategy Comparison")
    uploaded_files = st.file_uploader(
        "Upload Multiple Strategy Trade Logs",
        type=["csv", "xlsx"],
        accept_multiple_files=True,
        help="Upload trade logs for different strategies to compare",
    )

    if uploaded_files:
        strategies = {}
        for uploaded_file in uploaded_files:
            strategy_name = uploaded_file.name.split(".")[0]
            trades_df = load_trades_file(uploaded_file)

            if trades_df is not None and not trades_df.empty:
                stats = calculate_stats_from_trades(trades_df)
                strategies[strategy_name] = {"trades_df": trades_df, "stats": stats}

        if strategies:
            comparison_metrics = [
                "Net PnL",
                "Win Rate",
                "Max Drawdown",
                "Profit Factor",
                "Total Trades",
                "CAGR",
                "Calmar Ratio",
                "Average Return per Trade",
            ]

            comparison_data = []
            for name, strategy in strategies.items():
                stats = strategy["stats"]
                strategy_data = {
                    "Strategy": name,
                    "Net PnL": stats["Net PnL"],
                    "Win Rate": stats["Win Rate"] * 100,
                    "Max Drawdown": stats["Max Drawdown"],
                    "Profit Factor": stats["Profit Factor"],
                    "Total Trades": stats["Total Trades"],
                    "CAGR": stats["CAGR"],
                    "Calmar Ratio": stats["Calmar Ratio"],
                    "Average Return per Trade": stats["Average Return per Trade"],
                }
                comparison_data.append(strategy_data)

            comparison_df = pd.DataFrame(comparison_data)
            st.subheader("Strategy Performance Comparison")

            st.dataframe(
                comparison_df.set_index("Strategy").style.format(
                    {
                        "Net PnL": "₹{:.2f}",
                        "Win Rate": "{:.2f}%",
                        "Max Drawdown": "{:.2f}%",
                        "Profit Factor": "{:.2f}",
                        "CAGR": "{:.2f}",
                        "Calmar Ratio": "{:.2f}",
                        "Average Return per Trade": "₹{:.2f}",
                    }
                )
            )

            # Create comparison plots for key metrics
            for metric in comparison_metrics:
                fig = px.bar(
                    comparison_df,
                    x="Strategy",
                    y=metric,
                    title=f"Comparison of {metric}",
                    color_discrete_sequence=px.colors.qualitative.Bold,
                )
                st.plotly_chart(fig, use_container_width=True)


def main():
    st.set_page_config(page_title="Qode's Trading Strategy Analyzer", layout="wide")

    st.markdown(
        """
    <style>
    .css-1aumxhk {
        padding: 1rem;
        margin-top: 1rem;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    page = st.sidebar.radio(
        "Navigate", ["Single Strategy Analysis", "Strategy Comparison"]
    )

    if page == "Single Strategy Analysis":
        st.title("Qode's Trading Strategy Analyzer")
        st.write(
            """
            This app helps you analyze the performance of your trading strategies. 
            Upload your trade log and apply filters to analyze the performance metrics.
            Developed by Jay Jain.
            """
        )

        uploaded_file = st.file_uploader(
            "Upload Trade Log",
            type=["csv", "xlsx"],
            help="Upload your trade log in CSV or Excel format",
        )

        if uploaded_file is not None:
            trades_df = load_trades_file(uploaded_file)

            if trades_df is not None and not trades_df.empty:
                filtered_trades_df = apply_filters(trades_df)

                if not filtered_trades_df.empty:
                    stats = calculate_stats_from_trades(filtered_trades_df)

                    st.header("Performance Overview")
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric("Total Net PnL", f"₹{stats['Net PnL']:,.2f}")
                        st.metric("Win Rate", f"{stats['Win Rate']*100:.2f}%")
                        st.metric("Total Trades", stats["Total Trades"])

                    with col2:
                        st.metric(
                            "Return on Capital",
                            f"{stats['Return On Capital']:.2f}%",
                        )
                        st.metric("Profit Factor", f"{stats['Profit Factor']:.2f}")
                        st.metric(
                            "Avg Return per Trade",
                            f"₹{stats['Average Return per Trade']:,.2f}",
                        )

                    with col3:
                        st.metric(
                            "Max Drawdown",
                            f"{stats['Max Drawdown']:.2f}%",
                        )
                        st.metric("CAGR", f"{stats['CAGR']:.2f}")
                        st.metric("Calmar Ratio", f"{stats['Calmar Ratio']:.2f}")

                    st.header("Detailed Performance Analysis")

                    instrument_analysis(filtered_trades_df)

                else:
                    st.warning("No trades match the current filter criteria.")

    elif page == "Strategy Comparison":
        strategy_comparison_page()


if __name__ == "__main__":
    main()
