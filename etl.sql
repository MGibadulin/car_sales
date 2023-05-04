-- Create tables with card of car, for test  ETL
CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
AS SELECT
	card_id,
	title,
	price_primary,
	price_secondary,
	location, labels,
	comment,
	description,
	exchange,
	scrap_date
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card`
LIMIT 300;

CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_gallery_300`
AS SELECT
	card_id,
	ind, url,
	scrap_date
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_gallery`
LIMIT 300;

CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_options_300`
AS SELECT
	card_id,
	category,
	item,
	scrap_date
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_options`
LIMIT 300;

CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_url_300`
AS SELECT
	card_id,
	url,
	scrap_date
FROM `paid-project-346208`.car_ads_ds_landing.`lnd_cars-av-by_card_url`
LIMIT 300;

-- Create table for Stage 1
CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_landing`.`stg_cars-av-by_card`
(
	row_id			INT64 NOT NULL,
	row_hash		BYTES(32) NOT NULL,
	load_date		TIMESTAMP NOT NULL,
	modified_date	TIMESTAMP NOT NULL,
	card_id			STRING NOT NULL,
	brand			STRING NOT NULL,
	model			STRING NOT NULL,
	year			BYTES(2) NOT NULL,
	price			BYTES(4) NOT NULL,
	transmission 	STRING NOT NULL,
	mileage			BYTES(4) NOT NULL,
	body			STRING NOT NULL,
	engine			BYTES(2) NOT NULL,
	fuel 			STRING NOT NULL,
	exchange		STRING NOT NULL,
	top				STRING NOT NULL,
	vin				STRING NOT NULL,
	crahed			STRING NOT NULL,
	for_spare		STRING NOT NULL,
	comment			STRING NOT NULL,
	city			STRING NOT NULL,
	region			STRING NOT NULL,
	scrap_date		TIMESTAMP NOT NULL,
	PRIMARY KEY(row_id) NOT ENFORCED
);

SELECT
	GENERATE_UUID() AS row_ID,
	card_id,
	CASE
		WHEN split(title, ' ')[1] LIKE 'Lada' THEN 'Lada (ВАЗ)'
		WHEN split(title, ' ')[1] LIKE 'Alfa' THEN 'Alfa Romeo'
		WHEN split(title, ' ')[1] LIKE 'Dongfeng' THEN 'Dongfeng Honda'
		WHEN split(title, ' ')[1] LIKE 'Great' THEN 'Great Wall'
		WHEN split(title, ' ')[1] LIKE 'Iran' THEN 'Iran Khodro'
		WHEN split(title, ' ')[1] LIKE 'Land' THEN 'Land Rover'
		ELSE split(title, ' ')[1]
	END AS barnd,
	CASE
		WHEN split(title, ' ')[1] LIKE 'Lada' THEN REGEXP_EXTRACT(title, r'Продажа Lada \(ВАЗ\) (.+)[,]')
		WHEN split(title, ' ')[1] LIKE 'Alfa' THEN REGEXP_EXTRACT(title, r'Продажа Alfa Romeo (.+)[,]')
		WHEN split(title, ' ')[1] LIKE 'Dongfeng' THEN REGEXP_EXTRACT(title, r'Продажа Dongfeng Honda (.+)[,]')
		WHEN split(title, ' ')[1] LIKE 'Great' THEN REGEXP_EXTRACT(title, r'Продажа Great Wall (.+)[,]')
		WHEN split(title, ' ')[1] LIKE 'Iran' THEN REGEXP_EXTRACT(title, r'Продажа Iran Khodro (.+)[,]')
		WHEN split(title, ' ')[1] LIKE 'Land' THEN REGEXP_EXTRACT(title, r'Продажа Land Rover (.+)[,]')
		ELSE REGEXP_EXTRACT(title, r'Продажа [a-zA-Zа-яёА-ЯЁ]+ (.+)[,]')
	END AS model,
	CAST(REGEXP_EXTRACT(description, r'^(\d{4}) г.,') AS INT) AS year,
	REGEXP_EXTRACT(REPLACE(price_secondary, ' ', ''), r'≈(\d+)\$') AS price,
	REGEXP_EXTRACT(description, r',\s(\S+),.+') AS transmission,
	CAST(REPLACE(REGEXP_EXTRACT(description, r',\s(\d+[ ]?\d*) км'), " ", "") AS INT) AS mileage,
	REGEXP_EXTRACT(description, r'\| ([А-Яа-я0-9. ]+)') AS body,
	CAST(REGEXP_EXTRACT(REPLACE(split(description, ',')[2], '.', ''), r'[0-9]+') AS INT) * 100 AS engine_vol,
	CASE
		WHEN INSTR(exchange, 'Возможен обмен') <> 0 THEN 'Y'
		WHEN INSTR(exchange, 'Возможен обмен с моей доплатой') <> 0 THEN 'Y'
		WHEN INSTR(exchange, 'Возможен обмен с вашей доплатой') <> 0 THEN 'Y'
		WHEN INSTR(exchange, 'Обмен не интересует') <> 0 THEN 'N'
		ELSE 'N'
	END AS exchange,
	CASE
		WHEN INSTR(description, 'Запас хода') <> 0 THEN 'Electric'
		ELSE split(description, ',')[3]
	END AS fuel,
	CASE
		WHEN INSTR(labels, 'TOP') <> 0 THEN 'Y'
		ELSE 'N'
	END AS top,
	CASE
		WHEN INSTR(labels, 'VIN') <> 0 THEN 'Y'
		ELSE 'N'
	END as vin,
	CASE
		WHEN INSTR(labels, 'аварийный') <> 0 THEN 'Y'
		ELSE 'N'
	END AS crahed,
	CASE
		WHEN INSTR(labels, 'на запчасти') <> 0 THEN 'Y'
		ELSE 'N'
	END AS for_spare,
	CASE
		WHEN INSTR(location, ',') <> 0 THEN TRIM(split(location, ',')[0])
		WHEN location IS NULL THEN ''
		ELSE location
	END AS city,
	CASE
		WHEN INSTR(location, ',') <> 0 THEN TRIM(split(location, ',')[1])
		ELSE ''
	END AS region,
	CASE
		WHEN comment IS NULL THEN ''
		ELSE comment
	END AS comment
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`;

WITH t1 AS
(
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY card_id) AS cnt
	FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card`
)
SELECT card_id, title, description, scrap_date
FROM t1
WHERE t1.cnt > 1
ORDER BY card_id;




