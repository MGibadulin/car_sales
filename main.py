"""
A script extracts and processes data from a specific csv file with
data from a car listing website.
It enters the result into the standard output window.
"""

import argparse
from pathlib import Path


def get_args() -> argparse.Namespace:
    """Get args"""

    parser = argparse.ArgumentParser(
        description="A script extracts and processes data from \
                                 a specific csv file with data from a car listing website"
    )

    parser.add_argument("--brand", type=str, help="Vehicle manufacturer")
    parser.add_argument("--year_from", type=int, help="Build date vehicle from")
    parser.add_argument("--year_to", type=int, help="Build date vehicle to")
    parser.add_argument("--model", type=str, help="Model vehicle")
    parser.add_argument("--price_from", type=int, help="Minimal price in USD")
    parser.add_argument("--price_to", type=int, help="Maximal price in USD")
    parser.add_argument("--transmission", type=str, help="Type of transmission")
    parser.add_argument("--mileage", type=int, help="Maximal mileage")
    parser.add_argument("--body", type=str, help="Type of body vehicle")
    parser.add_argument(
        "--engine_from", type=int, help="Minimal volume of engine in cm^3"
    )
    parser.add_argument(
        "--engine_to", type=int, help="Maximal volume of engine in cm^3"
    )
    parser.add_argument("--fuel", type=str, help="Type of fuel")
    parser.add_argument("--exchange", type=str, help="Ready to exchange, yes/no")
    parser.add_argument(
        "--keywords",
        type=str,
        help="Any text you are looking for in the ad,\
                    independent keywords separated by commas",
    )
    parser.add_argument(
        "--max_records", type=int, default=20, help="Maximal number of records output"
    )

    args = parser.parse_args()
    return args


def load_data(file_path: Path) -> Path:
    """Load data from CSV file"""
    return file_path


if __name__ == "__main__":
    args_app = get_args()

    print(type(args_app))
