"""
A script extracts and processes data from a specific csv file with
data from a car listing website.
It enters the result into the standard output window.
"""

import argparse
import csv
from pathlib import Path
import pprint

BRANDS_TWO_WORDS = {
    "Lada": "Lada (ВАЗ)",
    "Alfa": "Alfa Romeo",
    "Dongfeng": "Dongfeng Honda",
    "Great": "Great Wall",
    "Iran": "Iran Khodro",
    "Renault": "Renault Samsung",
    "Land": "Land Rover",
}

EXCHANGE_MAP = {
    "Возможен обмен": "yes",
    "Обмен не интересует": "no",
    "Возможен обмен с моей доплатой": "yes",
    "Возможен обмен с вашей доплатой": "yes",
}


def get_args() -> argparse.Namespace:
    """Get args"""

    parser = argparse.ArgumentParser(
        description="A script extracts and processes data from \
                                 a specific csv file with data from a car listing website"
    )

    parser.add_argument("--brand", type=str, default=None, help="Vehicle manufacturer")
    parser.add_argument(
        "--year_from", type=int, default=0, help="Build date vehicle from"
    )
    parser.add_argument(
        "--year_to", type=int, default=2199, help="Build date vehicle to"
    )
    parser.add_argument("--model", type=str, default=None, help="Model vehicle")
    parser.add_argument(
        "--price_from", type=int, default=0, help="Minimal price in USD"
    )
    parser.add_argument(
        "--price_to", type=int, default=10**9, help="Maximal price in USD"
    )
    parser.add_argument("--transmission", type=str, help="Type of transmission")
    parser.add_argument("--mileage", type=int, default=10**6, help="Maximal mileage")
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
        default="",
        help="Any text you are looking for in the ad,\
                    independent keywords separated by commas",
    )
    parser.add_argument(
        "--max_records", type=int, default=50, help="Maximal number of records output"
    )

    args = parser.parse_args()
    return args


def extract_brand(input_string: str) -> str:
    """Extract field 'brand' from 'title'."""
    # Get second word from field - 'brand', first word is "Продажа"
    brand_from_title = input_string.split(" ", maxsplit=2)[1]
    if brand_from_title in BRANDS_TWO_WORDS:
        brand_from_title = BRANDS_TWO_WORDS[brand_from_title]
    return brand_from_title


def extract_model(input_string: str, brand: str) -> str:
    """Extract field 'model' from 'title'."""
    prefix = "Продажа " + brand
    model_from_title = input_string.removeprefix(prefix)
    model_from_title = model_from_title.rsplit(",", maxsplit=1)[0]
    model_from_title = model_from_title.strip()
    return model_from_title


def extract_price(input_string: str) -> int:
    """Extract field 'price' from 'price_secondary'."""
    price_from_field = "".join(ch for ch in input_string if ch.isdigit())
    return int(price_from_field)


def extract_year(input_string: str) -> int:
    """Extract field 'year' from 'description'."""
    year_from_field = input_string.split(" ", maxsplit=1)[0]
    return int(year_from_field)


def extract_transmission(input_string: str) -> str:
    """Extract field 'transmission' from 'description'."""
    transmission_from_field = input_string.split("|")[0]
    transmission_from_field = transmission_from_field.split(",", maxsplit=2)[1]
    transmission_from_field = transmission_from_field.strip()
    return transmission_from_field


def extract_engine(input_string: str) -> str:
    """Extract field 'engine' from 'description'."""
    engine_from_field = input_string.split("|")[0]
    engine_from_field = engine_from_field.split(",", maxsplit=3)[2]
    engine_from_field = "".join(ch for ch in input_string if ch.isdigit())
    # 1.6 л -> 16 -> 1600
    engine_from_field = 100 * int(engine_from_field)
    return engine_from_field


def extract_fuel(input_string: str) -> str:
    """Extract field 'fuel' from 'description'."""
    fuel_from_field = input_string.split("|")[0]
    fuel_from_field = fuel_from_field.split(",", maxsplit=4)[3]
    fuel_from_field = fuel_from_field.strip()
    return fuel_from_field


def extract_mileage(input_string: str) -> int:
    """Extract field 'mileage' from 'description'."""
    mileage_from_field = input_string.split("|")[0]
    mileage_from_field = mileage_from_field.strip().rsplit(",", maxsplit=1)[1]
    mileage_from_field = "".join(ch for ch in mileage_from_field if ch.isdigit())
    return int(mileage_from_field)


def extract_body(input_string: str) -> str:
    """Extract field 'body' from 'description'."""
    body_from_field = input_string.split("|")[1]
    body_from_field = body_from_field.split(",", maxsplit=1)[0]
    body_from_field = body_from_field.strip()
    return body_from_field


def extract_exchange(input_string: str) -> str:
    """Extract field 'exchange' from 'exchange' and convert yes/no"""
    exchange = EXCHANGE_MAP[input_string]
    return exchange


def is_valid_brand(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if param_filters.brand is None:
        return True
    if input_data["brand"] == param_filters.brand:
        return True
    return False


def is_valid_model(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if param_filters.brand is None:
        return True
    if input_data["model"] == param_filters.model:
        return True
    return False


def is_valid_price(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if input_data["price"] >= param_filters.price_from:
        if input_data["price"] <= param_filters.price_to:
            return True
        else:
            return False
    return False


def is_valid_year(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if input_data["year"] >= param_filters.year_from:
        if input_data["year"] <= param_filters.year_to:
            return True
        else:
            return False
    return False


def is_valid_transmission(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if param_filters.brand is None:
        return True
    if input_data["transmission"] == param_filters.transmission:
        return True
    return False


def is_valid_engine(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if param_filters.brand is None:
        return True
    if input_data["engine"] >= param_filters.engine_from:
        if input_data["engine"] <= param_filters.engine_to:
            return True
        else:
            return False
    return False


def is_valid_fuel(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if param_filters.brand is None:
        return True
    if input_data["fuel"] == param_filters.fuel:
        return True
    return False


def is_valid_mileage(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if input_data["mileage"] <= param_filters.mileage:
        return True
    return False


def is_valid_body(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if param_filters.brand is None:
        return True
    if input_data["body"] == param_filters.body:
        return True
    return False


def is_valid_exchange(input_data: dict, param_filters: argparse.Namespace) -> bool:
    if param_filters.brand is None:
        return True
    if input_data["exchange"] == param_filters.exchange:
        return True
    return False


def is_valid_keywords(input_data: dict, param_filters: argparse.Namespace) -> bool:
    return True


FILTERS_PIPELINE = (
    is_valid_brand,
    is_valid_model,
    is_valid_price,
    is_valid_year,
    is_valid_transmission,
    is_valid_engine,
    is_valid_fuel,
    is_valid_mileage,
    is_valid_body,
    is_valid_exchange,
    is_valid_keywords,
)


def load_data(file_path: Path):
    """Load data from csv file"""
    csv_file = open(file_path, encoding="utf-8")
    csv_reader = csv.DictReader(csv_file, delimiter=",", quotechar='"')
    return csv_reader


def extract_data(reader) -> None:
    """Process data from CSV file"""
    extracted_data = []
    for row in reader:  # Transform
        brand = extract_brand(row["title"])
        model = extract_model(row["title"], brand)
        price = extract_price(row["price_secondary"])
        # print(f"Title: {row['title']}, price: {row['price_secondary']}")
        # print(f" -> {brand} - {model} - {price}")

        year = extract_year(row["description"])
        transmission = extract_transmission(row["description"])
        engine = extract_engine(row["description"])
        fuel = extract_fuel(row["description"])
        mileage = extract_mileage(row["description"])
        body = extract_body(row["description"])
        exchange = extract_exchange(row["exchange"])
        # print(row["description"])
        # print(f" -> {year} - {transmission} - {engine} - {fuel} - {mileage} - {body}")
        extracted_data.append(
            {
                "brand": brand,
                "model": model,
                "price": price,
                "year": year,
                "transmission": transmission,
                "engine": engine,
                "fuel": fuel,
                "mileage": mileage,
                "body": body,
                "exchange": exchange,
                "card": row,
            }
        )
    return extracted_data


def filter_data(input_data: list, param_filters: argparse.Namespace) -> list:
    filtered_data = []
    for card in input_data:
        all_filter_valid = True
        for check_filter in FILTERS_PIPELINE:
            if check_filter(card, param_filters):
                continue
            all_filter_valid = False
            break
        if all_filter_valid:
            filtered_data.append(card)
    return filtered_data


def show_data(input_data: list, max_records):
    pprint.pprint(input_data[:max_records])


if __name__ == "__main__":
    args_app = get_args()
    reader = load_data("data/cars-av-by_card_v2.csv")
    extracted_data = extract_data(reader)
    filtered_data = filter_data(extracted_data, args_app)
    show_data(filtered_data, args_app.max_records)
