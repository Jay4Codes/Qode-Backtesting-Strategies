import pandas as pd
import logging
import os


# === CONFIG === #
if not os.path.exists("logs"):
    os.makedirs("logs")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/backtest_semi_directional.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

NIFTY_INDEX_FILE = "database/index/nifty_2024.parquet"
OPTIONS_FOLDER = "database/options/"
LOT_SIZE = 75
SLIPPAGE_PERCENT = 0.01
ATR_PERIOD = 14
START_TIME = "09:20:00"
END_TIME = "15:20:00"
EVENT_DAYS_TRADES = False
EVENT_DAYS_LIST = ["2024-03-02", "2024-05-18", "2024-06-03"]
PROFIT_TARGET = 0.15
STOP_LOSS = 0.08
HOLD_TIME = 90


class Position:
    def __init__(
        self,
        entry_timestamp,
        strike,
        option_type,
        entry_price,
        status,
        exit_price=None,
        exit_timestamp=None,
        exit_reason=None,
    ):
        self.entry_timestamp = entry_timestamp
        self.strike = strike
        self.option_type = option_type
        self.entry_price = entry_price
        self.status = status
        self.exit_price = exit_price
        self.exit_timestamp = exit_timestamp
        self.exit_reason = exit_reason


class Order:
    def __init__(self, timestamp, strike, option_type, price, side):
        self.timestamp = timestamp
        self.strike = strike
        self.option_type = option_type
        self.price = price
        self.side = side


# === LOAD NIFTY INDEX DATA === #
def load_nifty_index():
    df = pd.read_parquet(NIFTY_INDEX_FILE, engine="pyarrow")
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df


# === LOAD EXPIRY FOLDERS === #
def load_expiry_folders():
    return os.listdir(OPTIONS_FOLDER)


# === CALCULATE ATM & OTM STRIKES === #
def get_atm_otm_strikes(nifty_close):
    atm = round(nifty_close / 50) * 50
    otm = atm + 50

    return atm, otm


# === LOAD OPTIONS DATA === #
def load_option_data(expiry_folder, strike, option_type, timestamp):
    file_path = f"{OPTIONS_FOLDER}/{expiry_folder}/{strike}/NIFTY{expiry_folder}{strike}{option_type}.parquet"

    # logger.info(f"Loading: {file_path}")

    if os.path.exists(file_path):
        df = pd.read_parquet(
            file_path,
            engine="pyarrow",
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[df["timestamp"].dt.date == timestamp.date()]

        df = calculate_atr(df)
        df = calculate_vwap(df)

        if timestamp is not None:
            df = df[df["timestamp"] == timestamp]

        if not df.empty:
            return df.to_dict(orient="records")[0]
        else:
            logger.warning(f"Data Not Found: {file_path}")

    return None


# === ATR CALCULATION === #
def calculate_atr(df):
    df["high-low"] = df["high"] - df["low"]
    df["high-close"] = abs(df["high"] - df["close"].shift(1))
    df["low-close"] = abs(df["low"] - df["close"].shift(1))
    df["True Range"] = df[["high-low", "high-close", "low-close"]].max(axis=1)
    df["ATR"] = df["True Range"].expanding(min_periods=ATR_PERIOD).mean()

    return df


# === VWAP CALCULATION === #
def calculate_vwap(df):
    df["Typical Price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["Price * Volume"] = df["Typical Price"] * df["volume"]
    df["Cumulative Volume"] = df["volume"].cumsum()
    df["Cumulative Price * Volume"] = df["Price * Volume"].cumsum()
    df["VWAP"] = df["Cumulative Price * Volume"] / df["Cumulative Volume"]

    return df


def get_nearest_expiry(expiry_folders, timestamp):
    nearest_expiry_folder = None
    for expiry_folder in expiry_folders:
        expiry_date = pd.to_datetime(expiry_folder)
        if expiry_date >= timestamp:
            if nearest_expiry_folder is None or expiry_date < nearest_expiry_folder:
                nearest_expiry_folder = expiry_date

    nearest_expiry_folder = (
        pd.to_datetime(nearest_expiry_folder).strftime("%d%b%y").upper()
    )

    return nearest_expiry_folder


def enter_trade(row, atm_strike, otm_strike, atm_pe, otm_ce, positions, orders):
    logger.info(f"Entry: {row['timestamp']} - {otm_ce['open']}")

    entry_ce_price = otm_ce["open"] * SLIPPAGE_PERCENT + otm_ce["open"]
    logger.info(f"Entry Price: {entry_ce_price}")

    orders.append(
        Order(
            row["timestamp"],
            otm_strike,
            "CE",
            entry_ce_price,
            "BUY",
        )
    )

    positions.append(
        Position(
            entry_timestamp=row["timestamp"],
            strike=otm_strike,
            option_type="CE",
            entry_price=entry_ce_price,
            status=True,
        )
    )

    entry_pe_price = atm_pe["open"] * SLIPPAGE_PERCENT + atm_pe["open"]
    logger.info(f"Entry Price: {entry_pe_price}")

    orders.append(
        Order(
            row["timestamp"],
            atm_strike,
            "PE",
            entry_pe_price,
            "BUY",
        )
    )

    positions.append(
        Position(
            entry_timestamp=row["timestamp"],
            strike=atm_strike,
            option_type="PE",
            entry_price=entry_pe_price,
            status=True,
        )
    )


def check_exit_signal(
    expiry_folder, timestamp, positions, signal_value, otm_ce_price, atm_pe_price
):
    if timestamp > pd.to_datetime(f"{timestamp.date()} {END_TIME}"):
        return True, "EOD"

    active_positions = [position for position in positions if position.status]

    call_position = [
        position for position in active_positions if position.option_type == "CE"
    ][0]

    call_option = load_option_data(
        expiry_folder, call_position.strike, call_position.option_type, timestamp
    )

    if call_option:
        call_position_pnl = call_option["open"] - call_position.entry_price

    put_position = [
        position for position in active_positions if position.option_type == "PE"
    ][0]

    put_option = load_option_data(
        expiry_folder, put_position.strike, put_position.option_type, timestamp
    )

    if put_option:
        put_position_pnl = put_option["open"] - put_position.entry_price

    pnl = (call_position_pnl + put_position_pnl) / (
        call_position.entry_price + put_position.entry_price
    )

    if pnl >= PROFIT_TARGET:
        return True, "Profit Target Hit"

    if pnl <= -STOP_LOSS:
        return True, "Stop Loss Hit"

    if (timestamp - call_position.entry_timestamp).total_seconds() / 60 > HOLD_TIME:
        return True, "Hold Time Exceeded"

    if call_option["open"] + put_option["open"] < signal_value:
        return True, "Signal Reversed"

    return False, None


def exit_trade(expiry_folder, timestamp, positions, orders, exit_reason):
    logger.info(f"Exit: {timestamp} - {exit_reason}")

    call_position = [
        position
        for position in positions
        if position.status and position.option_type == "CE"
    ][0]

    call_option = load_option_data(
        expiry_folder, call_position.strike, call_position.option_type, timestamp
    )

    if call_option:
        call_position.exit_price = (
            call_option["open"] * SLIPPAGE_PERCENT + call_option["open"]
        )
        call_position.exit_timestamp = timestamp
        call_position.exit_reason = exit_reason
        call_position.status = False

        orders.append(
            Order(
                timestamp,
                call_position.strike,
                call_position.option_type,
                call_position.exit_price,
                "SELL",
            )
        )

    put_position = [
        position
        for position in positions
        if position.status and position.option_type == "PE"
    ][0]

    put_option = load_option_data(
        expiry_folder, put_position.strike, put_position.option_type, timestamp
    )

    if put_option:
        put_position.exit_price = (
            put_option["open"] * SLIPPAGE_PERCENT + put_option["open"]
        )
        put_position.exit_timestamp = timestamp
        put_position.exit_reason = exit_reason
        put_position.status = False

        orders.append(
            Order(
                timestamp,
                put_position.strike,
                put_position.option_type,
                put_position.exit_price,
                "SELL",
            )
        )


def save_results(positions, orders, trading_day):
    logger.info(f"Saving Results: {trading_day}")

    if not positions and not orders:
        return

    positions_df = pd.DataFrame(
        [
            {
                "Entry Timestamp": position.entry_timestamp,
                "Strike": position.strike,
                "Option Type": position.option_type,
                "Entry Price": position.entry_price,
                "Exit Price": position.exit_price,
                "Exit Timestamp": position.exit_timestamp,
                "Exit Reason": position.exit_reason,
                "PnL per Lot": (position.exit_price - position.entry_price) * LOT_SIZE,
                "Hold Time": (
                    position.exit_timestamp - position.entry_timestamp
                ).total_seconds()
                / 60,
                "Lot Size": LOT_SIZE,
                "Quantity": 1,
                "Cost per Lot": position.entry_price * LOT_SIZE * 0.002,
                "Net PnL per Lot": (
                    (position.exit_price - position.entry_price) * LOT_SIZE
                )
                - (position.entry_price * LOT_SIZE * 0.002),
            }
            for position in positions
        ]
    )

    orders_df = pd.DataFrame(
        [
            {
                "Timestamp": order.timestamp,
                "Strike": order.strike,
                "Option Type": order.option_type,
                "Price": order.price,
                "Side": order.side,
                "Lot Size": LOT_SIZE,
                "Quantity": 1,
            }
            for order in orders
        ]
    )

    os.makedirs("semi_directional_results", exist_ok=True)

    positions_df.to_csv(
        f"semi_directional_results/{trading_day}_positions.csv", index=False
    )
    orders_df.to_csv(f"semi_directional_results/{trading_day}_orders.csv", index=False)


# === BACKTESTING FUNCTION === #
def backtest(start_date, end_date):
    nifty_df = load_nifty_index()
    logger.info(f"Nifty Index Loaded: {nifty_df.shape}")

    expiry_folders = load_expiry_folders()
    logger.info(f"Expiry Folders: {len(expiry_folders)}")

    trading_days = nifty_df["timestamp"].dt.date.unique()

    trading_days = [
        day
        for day in trading_days
        if day >= pd.Timestamp(start_date).date()
        and day <= pd.Timestamp(end_date).date()
    ]

    if not EVENT_DAYS_TRADES:
        trading_days = [
            day
            for day in trading_days
            if day not in pd.to_datetime(EVENT_DAYS_LIST).date
        ]

    logger.info(f"Trading Days: {len(trading_days)}")

    for trading_day in trading_days:
        logger.info(f"Processing: {trading_day}")

        nifty_df_day = nifty_df[nifty_df.timestamp.dt.date == trading_day]

        logger.info(f"Nifty Index: {nifty_df_day.shape}")

        nearest_expiry_folder = get_nearest_expiry(
            expiry_folders, pd.to_datetime(trading_day)
        )
        logger.info(f"Nearest Expiry: {nearest_expiry_folder}")

        positions = []
        orders = []

        for _, row in nifty_df_day.iterrows():
            if row["timestamp"] < pd.to_datetime(f"{trading_day} {START_TIME}"):
                continue

            logger.info(f"Processing: {row['timestamp']} - {row['close']}")

            atm_strike, otm_strike = get_atm_otm_strikes(row["close"])
            logger.info(f"ATM: {atm_strike}, OTM: {otm_strike}")

            atm_ce = load_option_data(
                nearest_expiry_folder, atm_strike, "CE", row["timestamp"]
            )
            atm_pe = load_option_data(
                nearest_expiry_folder, atm_strike, "PE", row["timestamp"]
            )
            otm_ce = load_option_data(
                nearest_expiry_folder, otm_strike, "CE", row["timestamp"]
            )

            if atm_ce is None or atm_pe is None or otm_ce is None:
                continue

            logger.info(
                f"ATM CE Close: {atm_ce['close']} - Volume: {atm_ce['volume']} - ATR: {atm_ce['ATR']} - VWAP: {atm_ce['VWAP']}"
            )

            logger.info(
                f"ATM PE Close: {atm_pe['close']} - Volume: {atm_pe['volume']} - ATR: {atm_pe['ATR']} - VWAP: {atm_pe['VWAP']}"
            )

            logger.info(
                f"OTM CE Close: {otm_ce['close']} - Volume: {otm_ce['volume']} - ATR: {otm_ce['ATR']} - VWAP: {otm_ce['VWAP']}"
            )

            signal_value = otm_ce["VWAP"] + 0.5 * atm_ce["ATR"]

            logger.info(f"Signal Value: {signal_value}")

            logger.info(f"Positions: {len(positions)}")
            logger.info(f"Orders: {len(orders)}")

            # === ENTRY === #
            if (
                not any(position.status for position in positions)
                and otm_ce["open"] > signal_value
                and row["timestamp"] < pd.to_datetime(f"{trading_day} 15:20:00")
                and otm_ce["open"] + atm_pe["open"] > 50
            ):
                enter_trade(
                    row, atm_strike, otm_strike, atm_pe, otm_ce, positions, orders
                )
                continue

            # === EXIT === #
            if any(position.status for position in positions):
                exit_signal, exit_reason = check_exit_signal(
                    nearest_expiry_folder,
                    row["timestamp"],
                    positions,
                    signal_value,
                    otm_ce["open"],
                    atm_pe["open"],
                )

                if exit_signal:
                    exit_trade(
                        nearest_expiry_folder,
                        row["timestamp"],
                        positions,
                        orders,
                        exit_reason,
                    )

        logger.info(f"Positions: {len(positions)}")
        logger.info(f"Orders: {len(orders)}")

        save_results(positions, orders, trading_day)


# === RUN BACKTEST === #
if __name__ == "__main__":
    backtest("2024-01-01", "2024-12-31")
