"""
A script extracts and processes data from a specific csv file with
data from a car listing website.
It enters the result into the standard output window.
"""

import argparse
import csv
import io
from datetime import datetime
from pathlib import Path

from tabulate import tabulate

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
    "Возможен обмен с моей доплатой": "yes",
    "Возможен обмен с вашей доплатой": "yes",
    "Обмен не интересует": "no",
}


def get_args() -> argparse.Namespace:
    """Get args"""

    parser = argparse.ArgumentParser(
        description="A script extracts and processes data from \
                                 a specific csv file with data from a car listing website"
    )

    parser.add_argument(
        "--brand", type=str, default=None, help="Vehicle manufacturer"
    )
    parser.add_argument(
        "--year_from", type=int, default=0, help="Build date vehicle from"
    )
    parser.add_argument(
        "--year_to", type=int, default=2099, help="Build date vehicle to"
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
        "--engine_from", type=int, default=0, help="Minimal volume of engine in cm^3"
    )
    parser.add_argument(
        "--engine_to", type=int, default=10000, help="Maximal volume of engine in cm^3"
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
        "--max_records", type=int, default=20, help="Maximal number of records output"
    )

    args = parser.parse_args()
    return args


def extract_brand(input_string: str) -> str:
    """Extract field 'brand' from 'title'."""
    # Get second word from field - 'brand', first word is "Продажа"
    brand_from_title = input_string.split(" ", maxsplit=2)[1]
    if brand_from_title in BRANDS_TWO_WORDS:
        brand_from_title = BRANDS_TWO_WORDS[brand_from_title]
    return brand_from_title.strip()


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
    return transmission_from_field.strip()


def extract_engine(input_string: str) -> str:
    """Extract field 'engine' from 'description'."""
    engine_from_field = input_string.split("|")[0]
    engine_from_field = engine_from_field.split(",", maxsplit=3)[2].strip()
    if engine_from_field != "электро":
        engine_from_field = "".join(ch for ch in engine_from_field if ch.isdigit())
        try:
            # 1.6 л -> 16 -> 1600
            engine_from_field = 100 * int(engine_from_field)
        except:
            # если нет поля цифрами (объемом двигателя), то 0
            engine_from_field = 0
    else:
        # если электромобиль, то объем двигателя 0
        engine_from_field = 0
    return engine_from_field


def extract_fuel(input_string: str) -> str:
    """Extract field 'fuel' from 'description'."""
    # получаем поле что бы понять карточка авто с ДВС и электро
    description = input_string.split("|")[0]
    engine_from_field = description.split(",", maxsplit=3)[2].strip()
    if engine_from_field == "электро":
        return "электро"        
    else:
        fuel_from_field = description.split(",", maxsplit=4)[3]
        return fuel_from_field.strip()
        


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
    return body_from_field.strip()


def extract_exchange(input_string: str) -> str:
    """Extract field 'exchange' from 'exchange' and convert yes/no"""
    exchange = EXCHANGE_MAP[input_string]
    return exchange


def is_valid_brand(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Checking whether the field 'brand' equal the required value"""
    if param_filters.brand is None:
        return True
    if input_data["brand"] == param_filters.brand:
        return True
    return False


def is_valid_model(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Checking whether the field 'model' equal the required value"""
    if param_filters.model is None:
        return True
    if input_data["model"] == param_filters.model:
        return True
    return False


def is_valid_price(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Check if the "price" field contains a value within the required boundaries"""
    if input_data["price"] >= param_filters.price_from:
        if input_data["price"] <= param_filters.price_to:
            return True
        else:
            return False
    return False


def is_valid_year(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Check if the "year" field contains a value within the required boundaries"""
    if input_data["year"] >= param_filters.year_from:
        if input_data["year"] <= param_filters.year_to:
            return True
        else:
            return False
    return False


def is_valid_transmission(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Checking whether the field 'transmission' equal the required value"""
    if param_filters.transmission is None:
        return True
    if input_data["transmission"] == param_filters.transmission:
        return True
    return False


def is_valid_engine(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Check if the "engine" field contains a value within the required boundaries"""
    if input_data["engine"] >= param_filters.engine_from:
        if input_data["engine"] <= param_filters.engine_to:
            return True
        else:
            return False
    return False


def is_valid_fuel(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Checking whether the field 'fuel' equal the required value"""
    if param_filters.fuel is None:
        return True
    if input_data["fuel"] == param_filters.fuel:
        return True
    return False


def is_valid_mileage(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Checking whether the field 'mileage' equal or less value then the required value"""
    if input_data["mileage"] <= param_filters.mileage:
        return True
    return False


def is_valid_body(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Checking whether the field 'body' equal the required value"""
    if param_filters.body is None:
        return True
    if input_data["body"] == param_filters.body:
        return True
    return False


def is_valid_exchange(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Checking whether the field 'exchange' equal the required value"""
    if param_filters.exchange is None:
        return True
    if input_data["exchange"] == param_filters.exchange:
        return True
    return False


def is_valid_keywords(input_data: dict, param_filters: argparse.Namespace) -> bool:
    """Checking whether a string contains keywords from a list"""
    # Input data is a string of comma-separated words
    keywords = param_filters.keywords.split(",")
    for word in keywords:
        if word.strip() in input_data["card"]:
            return True
    return False


def load_data(file_path: Path):
    """Load data from file"""
    file = open(file_path, encoding="utf-8")
    input_data = file.read()
    input_data = io.StringIO(input_data)
    return input_data


def extract_data(input_data) -> None:
    """Extract fields from CSV data"""
    reader = csv.DictReader(input_data, delimiter=",", quotechar='"')
    extracted_data = []
    for row in reader:
        brand = extract_brand(row["title"])
        model = extract_model(row["title"], brand)
        price = extract_price(row["price_secondary"])
        year = extract_year(row["description"])
        transmission = extract_transmission(row["description"])
        engine = extract_engine(row["description"])
        fuel = extract_fuel(row["description"])
        mileage = extract_mileage(row["description"])
        body = extract_body(row["description"])
        exchange = extract_exchange(row["exchange"])

        # Deleting fields that do not need to be searched
        row.pop("card_id"),
        row.pop("scrap_date")
        # Save serialized card for future search by keywords
        card = ",".join(row.values())

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
                "card": card,
            }
        )
    return extracted_data

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


def filter_data(input_data: list, param_filters: argparse.Namespace) -> list:
    """Filtering rows by parameters"""
    filtered_data = []
    for row in input_data:
        all_filter_valid = True
        for check_filter in FILTERS_PIPELINE:
            if check_filter(row, param_filters):
                continue
            all_filter_valid = False
            break
        if all_filter_valid:
            row.pop("card")
            filtered_data.append(row)
    return filtered_data


def order_data(input_data:list):
    """Sorting the data into three columns"""
    input_data.sort(key=lambda row: (row["price"], -row["year"], row["mileage"]))


def load_out_data(input_data: list, max_records):
    """Print date in table format"""
    print(tabulate(input_data[:max_records], headers="keys"))
    
def main():
    """Main function"""
    start_time = datetime.now()
    
    args_app = get_args()
    ts_args_app = datetime.now()
    
    input_data = load_data("data/cars-av-by_card_v2.csv")
    ts_load = datetime.now()
    
    extracted_data = extract_data(input_data)
    ts_extract = datetime.now()
    
    filtered_data = filter_data(extracted_data, args_app)
    ts_filter = datetime.now()
    
    order_data(filtered_data)
    ts_order = datetime.now()
    
    load_out_data(filtered_data, args_app.max_records)
    ts_show = datetime.now()

    print(f"Parse args: {ts_args_app - start_time}")
    print(f"Load: {ts_load - ts_args_app}")
    print(f"Extract: {ts_extract - ts_load}")
    print(f"Filter: {ts_filter - ts_extract}")
    print(f"Order: {ts_order - ts_extract}")
    print(f"Show: {ts_show - ts_order}")
    print(f"Total: {ts_show - start_time}")
    
    
if __name__ == "__main__":
    main()
    