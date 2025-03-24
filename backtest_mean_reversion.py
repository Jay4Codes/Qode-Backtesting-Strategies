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
        logging.FileHandler("logs/backtest_mean_reversion.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

NIFTY_INDEX_FILE = "database/index/nifty_2024.parquet"
OPTIONS_FOLDER = "database/options/"
LOT_SIZE = 75
SLIPPAGE_PERCENT = 0.01
BB_PERIOD = 20
RSI_PERIOD = 14
START_TIME = "09:20:00"
END_TIME = "15:20:00"
EVENT_DAYS_TRADES = False
EVENT_DAYS_LIST = ["2024-03-02", "2024-05-18", "2024-06-03"]
PROFIT_TARGET = 0.2
STOP_LOSS = 0.1
HOLD_TIME = 120


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


# === CALCULATE ATM STRIKE === #
def get_atm_strike(nifty_close):
    atm = round(nifty_close / 50) * 50

    return atm


# === LOAD OPTIONS DATA === #
def load_option_data(expiry_folder, strike, option_type, timestamp):
    file_path = f"{OPTIONS_FOLDER}/{expiry_folder}/{strike}/NIFTY{expiry_folder}{strike}{option_type}.parquet"

    # logger.info(f"Loading: {file_path}")

    if os.path.exists(file_path):
        df = pd.read_parquet(
            file_path,
            engine="pyarrow",
            columns=["timestamp", "open", "high", "low", "close"],
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df[df["timestamp"].dt.date == timestamp.date()]

        df = calculate_bollinger_bands(df)
        df = calculate_rsi(df)

        if timestamp is not None:
            df = df[df["timestamp"] == timestamp]

        if not df.empty:
            return df.to_dict(orient="records")[0]
        else:
            logger.warning(f"Data Not Found: {file_path}")

    return None


# === BOLLINGER BANDS CALCULATION === #
def calculate_bollinger_bands(df):
    df["SMA"] = df["close"].rolling(BB_PERIOD).mean()
    df["StdDev"] = df["close"].rolling(BB_PERIOD).std()
    df["UpperBB"] = df["SMA"] + (2 * df["StdDev"])
    df["LowerBB"] = df["SMA"] - (2 * df["StdDev"])
    return df


# === RSI CALCULATION === #
def calculate_rsi(df):
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(RSI_PERIOD).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(RSI_PERIOD).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))
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


def enter_trade(row, atm_strike, atm_pe, atm_ce, positions, orders):
    logger.info(f"Entry: {row['timestamp']} - {atm_ce['open']}")

    entry_ce_price = atm_ce["open"] * SLIPPAGE_PERCENT + atm_ce["open"]
    logger.info(f"Entry Price: {entry_ce_price}")

    orders.append(
        Order(
            row["timestamp"],
            atm_strike,
            "CE",
            entry_ce_price,
            "SELL",
        )
    )

    positions.append(
        Position(
            entry_timestamp=row["timestamp"],
            strike=atm_strike,
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
            "SELL",
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


def check_exit_signal(expiry_folder, timestamp, positions):
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
        call_position_pnl = call_position.entry_price - call_option["open"]

    put_position = [
        position for position in active_positions if position.option_type == "PE"
    ][0]

    put_option = load_option_data(
        expiry_folder, put_position.strike, put_position.option_type, timestamp
    )

    if put_option:
        put_position_pnl = put_position.entry_price - put_option["open"]

    pnl = (call_position_pnl + put_position_pnl) / (
        call_position.entry_price + put_position.entry_price
    )

    if pnl >= PROFIT_TARGET:
        return True, "Profit Target Hit"

    if pnl <= -STOP_LOSS:
        return True, "Stop Loss Hit"

    if call_option["RSI"] < 30:
        return True, "RSI Oversold"

    if (timestamp - call_position.entry_timestamp).total_seconds() / 60 >= HOLD_TIME:
        return True, "Hold Time Exceeded"

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
                "PnL per Lot": (position.entry_price - position.exit_price) * LOT_SIZE,
                "Hold Time": (
                    position.exit_timestamp - position.entry_timestamp
                ).total_seconds()
                / 60,
                "Lot Size": LOT_SIZE,
                "Quantity": 1,
                "Cost per Lot": position.entry_price * LOT_SIZE * 0.02,
                "Net PnL per Lot": (
                    (position.entry_price - position.exit_price) * LOT_SIZE
                )
                - (position.entry_price * LOT_SIZE * 0.02),
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

    os.makedirs("mean_reversion_results", exist_ok=True)

    positions_df.to_csv(
        f"mean_reversion_results/{trading_day}_positions.csv", index=False
    )
    orders_df.to_csv(f"mean_reversion_results/{trading_day}_orders.csv", index=False)


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

            atm_strike = get_atm_strike(row["close"])
            logger.info(f"ATM: {atm_strike}")

            atm_ce = load_option_data(
                nearest_expiry_folder, atm_strike, "CE", row["timestamp"]
            )
            atm_pe = load_option_data(
                nearest_expiry_folder, atm_strike, "PE", row["timestamp"]
            )

            if atm_ce is None or atm_pe is None:
                continue

            logger.info(
                f"ATM CE Close: {atm_ce['close']} - SMA: {atm_ce['SMA']} - RSI: {atm_ce['RSI']} - UpperBB: {atm_ce['UpperBB']}"
            )

            logger.info(
                f"ATM PE Close: {atm_pe['close']} - SMA: {atm_pe['SMA']} - RSI: {atm_pe['RSI']} - UpperBB: {atm_pe['UpperBB']}"
            )

            logger.info(f"Positions: {len(positions)}")
            logger.info(f"Orders: {len(orders)}")

            # === ENTRY === #
            if (
                not any(position.status for position in positions)
                and atm_ce["RSI"] > 70
                and atm_ce["open"] >= atm_ce["UpperBB"]
                and row["timestamp"] < pd.to_datetime(f"{trading_day} 15:20:00")
                and atm_ce["open"] + atm_pe["open"] > 50
            ):
                enter_trade(row, atm_strike, atm_pe, atm_ce, positions, orders)
                continue

            # === EXIT === #
            if any(position.status for position in positions):
                exit_signal, exit_reason = check_exit_signal(
                    nearest_expiry_folder,
                    row["timestamp"],
                    positions,
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
