# Searching for car ads
The script for searching data and displaying it in the standard output window according to the following criteria (command line arguments) 

# Description
A script extracts and processes data from a specific csv file with data from a car listing website https://cars.av.by.
It enters the result into the standard output window. Filter output according to the following command line arguments:

**-brand** Vehicle manufacturer

**-model** Model vehicle

**-year_from** Build date vehicle from

**-year_to** Build date vehicle to

**-price_from** Minimal price in USD

**-price_to** Maximal price in USD

**-transmission** Type of transmission

**-mileage** Maximal mileage

**-body** Type of body vehicle

**-engine_from** Minimal volume of engine in cm^3

**-engine_to** Maximal volume of engine in cm^3

**-fuel** Type of fuel

**-exchange** Ready to exchange, yes/no

**-keywords** Any text you are looking for in the ad independent keywords separated by commas

**-max_records** Maximal number of records output

**-file** Path to file with data

Sorting output by

Parsing data from website is over scope of project.
The main requirement is that no third-party libraries are used.

# Requirements
Python >= 3.8

# Usage
Put CSV file in 
```console
./data
```
Run script with arguments:
```console
~/car_ads$ python3 app.py -price_from 100000 -mileage 1000 -max_records 5
```
Output:
```console
brand       model                 price    year  transmission      engine  fuel      mileage  body                       exchange
----------  ------------------  -------  ------  --------------  --------  ------  ---------  -------------------------  ----------
Lexus       RX IV · Рестайлинг   102000    2022  автомат             3500  бензин        800  внедорожник 5 дв.          no
Lexus       RX IV · Рестайлинг   104000    2022  автомат             3500  бензин         70  внедорожник 5 дв.          no
Volkswagen  Multivan T6.1        104428    2022  автомат             2000  дизель          9  микроавтобус пассажирский  yes
BMW         X1 U11               107328    2022  автомат             2000  дизель         10  внедорожник 5 дв.          yes
Volkswagen  Multivan T7          108000    2022  робот               2000  дизель         51  микроавтобус пассажирский  no

