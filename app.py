""""""
import argparse
from pathlib import Path
import carsetl as etl
import logging
import time
import os


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
        default="data/cars-av-by_card_v3.csv",
        help="Path to file with data",
    )
    args = parser.parse_args()
    return vars(args)


def main():
    """Main function"""

    logging.basicConfig(
        level=logging.INFO,
        filename="log.log",
        encoding="utf-8",
        format=" %(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info(f"Parse args")
    start_time = time.perf_counter()
    args_app = get_args()

    file_name = Path(args_app["file"].stem + "_parsed" + args_app["file"].suffix)
    file_path = Path("data", file_name)
    need_tokenize = True
    if file_path.is_file():
        need_tokenize = False
    else:
        file_path = args_app["file"]

    input_data = etl.load_data(file_path)
    logging.info(f"Load file {file_path}")

    file_stats = os.stat(file_path)
    logging.info(f"File Size is {(file_stats.st_size / (1024 * 1024)):.2f} Mb")

    if need_tokenize:
        tokenized_data = etl.tokenize_data(input_data)

        file_name = Path(args_app["file"].stem + "_parsed" + args_app["file"].suffix)
        file_path = Path("data", file_name)
        logging.info(f"Save tokenized data")
        etl.save_data(file_path, tokenized_data)
    else:
        logging.info(f"Data already tokenized")
        tokenized_data = etl.convert_tokenized_data(input_data)

    logging.info(f"Filter data")
    filtered_data = etl.filter_data(tokenized_data, args_app)

    logging.info(f"Order data")
    etl.order_data(filtered_data)

    etl.load_out_data(filtered_data, args_app["max_records"])
    end_time = time.perf_counter()
    logging.info(f"Time elapsed on ETL {(end_time - start_time):.3f}")


if __name__ == "__main__":
    main()
