import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objs as go
from datetime import datetime, time
import sys
import os

sys.path.append(os.getcwd())
from summary import calculate_stats_from_trades


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
    st.sidebar.header("Advanced Filters")

    quantity_multiplier = st.sidebar.number_input(
        "Quantity Multiplier",
        min_value=1.0,
        value=1.0,
        step=0.5,
        help="Multiply the PnL and other metrics by this factor",
    )

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

    trades_df["Entry Time"] = trades_df["Entry Timestamp"].dt.time
    time_filter = st.sidebar.checkbox(
        "Filter by Trading Hours (9:15 AM to 3:00 PM)", value=True
    )
    if time_filter:
        trades_df = trades_df[
            (trades_df["Entry Time"] >= time(9, 15))
            & (trades_df["Entry Time"] <= time(15, 0))
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

    return trades_df


def instrument_analysis(trades_df):
    st.header("🔬 Instrument Performance Analysis")

    tabs = st.tabs(
        [
            "General Plots",
            "Detailed Metrics",
            "Net PnL vs Days to Expiry",
            "Instrument-wise Performance",
        ]
    )

    with tabs[0]:
        create_advanced_visualizations(trades_df)

    with tabs[3]:
        if "Instruments" in trades_df.columns:
            instrument_performance = (
                trades_df.groupby("Instruments")
                .agg(
                    {
                        "Net PnL per Lot": ["sum", "mean", "min", "max"],
                        "Instruments": "count",
                    }
                )
                .reset_index()
            )
            instrument_performance.columns = [
                "Instruments",
                "Total Net PnL",
                "Total Trades",
                "Average Net PnL",
                "Min Net PnL",
                "Max Net PnL",
            ]

            fig = px.bar(
                instrument_performance,
                x="Instruments",
                y="Total Net PnL",
                hover_data=["Total Trades", "Average Net PnL"],
                title="Net PnL by Instrument",
                color="Total Net PnL",
                color_continuous_scale=px.colors.sequential.Viridis,
            )
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(instrument_performance)

    with tabs[2]:
        if "Days to Expiry" in trades_df.columns:
            fig_scatter = px.scatter(
                trades_df,
                x="Days to Expiry",
                y="Net PnL per Lot",
                color="Instruments",
                hover_data=["Entry Timestamp", "Instruments"],
                title="Net PnL vs Days to Expiry",
                labels={
                    "Days to Expiry": "Days to Expiry",
                    "Net PnL per Lot": "Net PnL per Lot",
                },
                color_discrete_sequence=px.colors.qualitative.Plotly,
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

    with tabs[1]:
        if "Instruments" in trades_df.columns:
            instrument_metrics = (
                trades_df.groupby("Instruments")
                .agg(
                    {
                        "Net PnL per Lot": ["sum", "mean", "count"],
                        "PnL per Lot": ["mean"],
                        "Hold Time": ["mean"],
                        "Entry Price": ["max"],
                        "Exit Price": ["max"],
                    }
                )
                .reset_index()
            )

            instrument_metrics.columns = [
                "Instruments",
                "Total Net PnL",
                "Avg Net PnL",
                "Total Trades",
                "Avg PnL",
                "Avg Hold Time",
                "Max Entry Price",
                "Max Exit Price",
            ]

            st.dataframe(
                instrument_metrics.style.format(
                    {
                        "Total Net PnL": "{:.2f}",
                        "Avg Net PnL": "{:.2f}",
                        "Avg PnL": "{:.2f}",
                        "Avg Hold Time": "{:.2f}",
                        "Max Entry Price": "{:.2f}",
                        "Max Exit Price": "{:.2f}",
                    }
                )
            )


def equity_curve(trades_df):
    trades_df = trades_df.sort_values("Entry Timestamp")
    trades_df["Cumulative PnL"] = trades_df["Net PnL per Lot"].cumsum()

    fig_equity_curve = go.Figure()
    fig_equity_curve.add_trace(
        go.Scatter(
            x=trades_df["Entry Timestamp"],
            y=trades_df["Cumulative PnL"],
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
                stats = calculate_stats_from_trades(trades_df, starting_capital=100000)
                strategies[strategy_name] = {"trades_df": trades_df, "stats": stats}

        if strategies:
            comparison_data = []
            for name, strategy in strategies.items():
                stats = strategy["stats"]
                comparison_data.append(
                    {
                        "Strategy": name,
                        "Net PnL": stats["Net PnL"],
                        "Win Rate": stats["Win Rate"] * 100,
                        "Max Drawdown": stats["Max Drawdown on Capital"] * 100,
                        "Profit Factor": stats["Profit Factor"],
                        "Total Trades": stats["Total Trades"],
                    }
                )

            comparison_df = pd.DataFrame(comparison_data)
            st.subheader("Strategy Performance Comparison")

            metrics_to_compare = [
                "Net PnL",
                "Win Rate",
                "Max Drawdown",
                "Profit Factor",
                "Total Trades",
            ]

            for metric in metrics_to_compare:
                fig = px.bar(
                    comparison_df,
                    x="Strategy",
                    y=metric,
                    title=f"Comparison of {metric}",
                    color_discrete_sequence=px.colors.qualitative.Bold,
                )
                st.plotly_chart(fig, use_container_width=True)

            st.dataframe(comparison_df.set_index("Strategy"))


def main():
    st.set_page_config(page_title="Qode's Trading Strategy Analyzer", layout="wide")

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

        starting_capital = st.sidebar.number_input(
            "Starting Capital", min_value=0.0, value=100000.0, step=1000.0
        )

        if uploaded_file is not None:
            trades_df = load_trades_file(uploaded_file)

            if trades_df is not None and not trades_df.empty:
                filtered_trades_df = apply_filters(trades_df)

                if not filtered_trades_df.empty:
                    stats = calculate_stats_from_trades(
                        filtered_trades_df, starting_capital
                    )

                    st.header("Performance Overview")
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.metric("Total Net PnL", f"₹{stats['Net PnL']:,.2f}")
                        st.metric("Win Rate", f"{stats['Win Rate']*100:.2f}%")
                        st.metric("Total Trades", stats["Total Trades"])

                    with col2:
                        st.metric(
                            "Return on Capital",
                            f"{stats['Return On Capital']*100:.2f}%",
                        )
                        st.metric("Profit Factor", f"{stats['Profit Factor']:.2f}")
                        st.metric(
                            "Avg Return per Trade",
                            f"₹{stats['Average Return per Trade']:,.2f}",
                        )

                    with col3:
                        st.metric(
                            "Max Drawdown",
                            f"{stats['Max Drawdown on Capital']*100:.2f}%",
                        )
                        st.metric("CAGR", f"{stats['CAGR']*100:.2f}%")
                        st.metric("Calmar Ratio", f"{stats['Calmar Ratio']:.2f}")

                    st.header("Detailed Performance Analysis")

                    instrument_analysis(filtered_trades_df)

                else:
                    st.warning("No trades match the current filter criteria.")

    elif page == "Strategy Comparison":
        strategy_comparison_page()


if __name__ == "__main__":
    main()
