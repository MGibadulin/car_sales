""""""
import argparse
from pathlib import Path
import carsetl as etl
from datetime import datetime


def get_args() -> dict:
    """Get args"""

    parser = argparse.ArgumentParser(
        description="A script extracts and processes data from \
                                 a specific csv file with data from a car listing website"
    )

    parser.add_argument("-brand", type=str, default=None, help="Vehicle manufacturer")
    parser.add_argument(
        "-year_from", type=int, default=0, help="Build date vehicle from"
    )
    parser.add_argument(
        "-year_to", type=int, default=2099, help="Build date vehicle to"
    )
    parser.add_argument("-model", type=str, default=None, help="Model vehicle")
    parser.add_argument("-price_from", type=int, default=0, help="Minimal price in USD")
    parser.add_argument(
        "-price_to", type=int, default=10**9, help="Maximal price in USD"
    )
    parser.add_argument("-transmission", type=str, help="Type of transmission")
    parser.add_argument("-mileage", type=int, default=10**6, help="Maximal mileage")
    parser.add_argument("-body", type=str, help="Type of body vehicle")
    parser.add_argument(
        "-engine_from", type=int, default=0, help="Minimal volume of engine in cm^3"
    )
    parser.add_argument(
        "-engine_to", type=int, default=10000, help="Maximal volume of engine in cm^3"
    )
    parser.add_argument("-fuel", type=str, help="Type of fuel")
    parser.add_argument("-exchange", type=str, help="Ready to exchange, yes/no")
    parser.add_argument(
        "-keywords",
        type=str,
        default="",
        help="Any text you are looking for in the ad,\
                    independent keywords separated by commas",
    )
    parser.add_argument(
        "-max_records", type=int, default=20, help="Maximal number of records output"
    )
    parser.add_argument(
        "-file",
        type=Path,
        default="data/cars-av-by_card_v2.csv",
        help="Path to file with data",
    )
    args = parser.parse_args()
    return vars(args)


def main():
    """Main function"""
    start_time = datetime.now()

    args_app = get_args()
    ts_args_app = datetime.now()

    input_data = etl.load_data(args_app["file"])
    ts_load = datetime.now()

    extracted_data = etl.extract_data(input_data)
    ts_extract = datetime.now()

    filtered_data = etl.filter_data(extracted_data, args_app)
    ts_filter = datetime.now()

    etl.order_data(filtered_data)
    ts_order = datetime.now()

    etl.load_out_data(filtered_data, args_app["max_records"])
    ts_show = datetime.now()

    print(f"Parse args: {ts_args_app - start_time}")
    print(f"Load: {ts_load - ts_args_app}")
    print(f"Extract: {ts_extract - ts_load}")
    print(f"Filter: {ts_filter - ts_extract}")
    print(f"Order: {ts_order - ts_filter}")
    print(f"Show: {ts_show - ts_order}")
    print(f"Total: {ts_show - start_time}")


if __name__ == "__main__":
    main()
