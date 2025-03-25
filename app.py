import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objs as go
from datetime import datetime
import sys
import os

# Import the summary functions
sys.path.append(os.getcwd())
from summary import calculate_stats_from_trades


def load_trades_file(uploaded_file):
    """
    Load trades file with enhanced preprocessing
    """
    try:
        # Support multiple file types
        if uploaded_file.name.endswith(".csv"):
            trades_df = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith(".xlsx"):
            trades_df = pd.read_excel(uploaded_file)
        else:
            st.error("Unsupported file type. Please upload CSV or Excel.")
            return None

        # Convert timestamp columns
        timestamp_columns = ["Entry Timestamp", "Exit Timestamp"]
        for col in timestamp_columns:
            if col in trades_df.columns:
                trades_df[col] = pd.to_datetime(trades_df[col])

        # Ensure additional columns exist
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
    """
    Apply user-selected filters to the trades dataframe
    """
    # Sidebar filters
    st.sidebar.header("Advanced Filters")

    # Quantity Multiplier
    quantity_multiplier = st.sidebar.number_input(
        "Quantity Multiplier",
        min_value=1.0,
        value=1.0,
        step=0.5,
        help="Multiply the PnL and other metrics by this factor",
    )

    # Days to Expiry Filter
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

    # Expiry Day Flag Filter
    if "Expiry Day Flag" in trades_df.columns:
        expiry_day_filter = st.sidebar.multiselect(
            "Expiry Day Trades", [True, False], default=[True, False]
        )
        trades_df = trades_df[trades_df["Expiry Day Flag"].isin(expiry_day_filter)]

    # Month Filter
    if "Month" in trades_df.columns:
        month_filter = st.sidebar.multiselect(
            "Select Months",
            sorted(trades_df["Month"].unique()),
            default=sorted(trades_df["Month"].unique()),
        )
        trades_df = trades_df[trades_df["Month"].isin(month_filter)]

    # Hold Time Filter
    if "Hold Time" in trades_df.columns:
        min_hold = trades_df["Hold Time"].min()
        max_hold = trades_df["Hold Time"].max()
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

    # Exit Reason Filter
    if "Exit Reason" in trades_df.columns:
        exit_reason_filter = st.sidebar.multiselect(
            "Exit Reasons",
            trades_df["Exit Reason"].unique(),
            default=trades_df["Exit Reason"].unique(),
        )
        trades_df = trades_df[trades_df["Exit Reason"].isin(exit_reason_filter)]

    # PnL Filter
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

    # Adjust metrics based on quantity multiplier
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


def enhanced_instrument_analysis(trades_df):
    """
    Provide detailed analysis of trading instruments
    """
    if "Instruments" in trades_df.columns:
        # Instrument-wise Performance
        instrument_performance = (
            trades_df.groupby("Instruments")
            .agg({"Net PnL per Lot": "sum", "Instruments": "count"})
            .rename(columns={"Instruments": "Total Trades"})
        )

        st.subheader("Instrument-wise Performance")
        st.dataframe(instrument_performance)

        # Instrument Performance Visualization
        fig = px.bar(
            instrument_performance.reset_index(),
            x="Instruments",
            y="Net PnL per Lot",
            title="Net PnL by Instrument Combination",
        )
        st.plotly_chart(fig, use_container_width=True)


def main():
    st.title("Qode's Sauda Analyzer, Developed by Jay Jain")

    # Sidebar for file upload and configuration
    uploaded_file = st.sidebar.file_uploader(
        "Upload Trade Log",
        type=["csv", "xlsx"],
        help="Upload your trade log in CSV or Excel format",
    )

    starting_capital = st.sidebar.number_input(
        "Starting Capital", min_value=0.0, value=100000.0, step=1000.0
    )

    if uploaded_file is not None:
        # Load trades
        trades_df = load_trades_file(uploaded_file)

        if trades_df is not None and not trades_df.empty:
            # Apply filters
            filtered_trades_df = apply_filters(trades_df)

            if not filtered_trades_df.empty:
                # Calculate performance statistics
                stats = calculate_stats_from_trades(
                    filtered_trades_df, starting_capital
                )

                # Performance Overview (similar to previous version)
                st.header("Performance Overview")

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Total Net PnL", f"₹{stats['Net PnL']:,.2f}")
                    st.metric("Win Rate", f"{stats['Win Rate']*100:.2f}%")
                    st.metric("Total Trades", stats["Total Trades"])

                with col2:
                    st.metric(
                        "Return on Capital", f"{stats['Return On Capital']*100:.2f}%"
                    )
                    st.metric("Profit Factor", f"{stats['Profit Factor']:.2f}")
                    st.metric(
                        "Avg Return per Trade",
                        f"₹{stats['Average Return per Trade']:,.2f}",
                    )

                with col3:
                    st.metric(
                        "Max Drawdown", f"{stats['Max Drawdown on Capital']*100:.2f}%"
                    )
                    st.metric("CAGR", f"{stats['CAGR']*100:.2f}%")
                    st.metric("Calmar Ratio", f"{stats['Calmar Ratio']:.2f}")

                # Tabs for different analyses
                tab1, tab2, tab3, tab4 = st.tabs(
                    [
                        "Cumulative PnL",
                        "Instrument Analysis",
                        "Time & Expiry Analysis",
                        "Detailed Metrics",
                    ]
                )

                with tab1:
                    # Cumulative PnL Plot
                    filtered_trades_df["Cumulative PnL"] = filtered_trades_df[
                        "Net PnL per Lot"
                    ].cumsum()
                    fig = px.line(
                        filtered_trades_df,
                        x="Entry Timestamp",
                        y="Cumulative PnL",
                        title="Cumulative P&L Over Time",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with tab2:
                    # Enhanced Instrument Analysis
                    enhanced_instrument_analysis(filtered_trades_df)

                with tab3:
                    # Days to Expiry Analysis
                    if "Days to Expiry" in filtered_trades_df.columns:
                        # Days to Expiry vs PnL
                        fig_expiry = px.scatter(
                            filtered_trades_df,
                            x="Days to Expiry",
                            y="Net PnL per Lot",
                            title="Net PnL vs Days to Expiry",
                        )
                        st.plotly_chart(fig_expiry, use_container_width=True)

                    # Expiry Day Performance
                    if "Expiry Day Flag" in filtered_trades_df.columns:
                        expiry_performance = filtered_trades_df.groupby(
                            "Expiry Day Flag"
                        )["Net PnL per Lot"].agg(["sum", "count"])
                        st.subheader("Expiry Day vs Non-Expiry Day Performance")
                        st.dataframe(expiry_performance)

                with tab4:
                    # Detailed Metrics Table
                    metric_df = pd.DataFrame.from_dict(
                        stats, orient="index", columns=["Value"]
                    )
                    st.dataframe(metric_df)
            else:
                st.warning("No trades match the current filter criteria.")


if __name__ == "__main__":
    main()
