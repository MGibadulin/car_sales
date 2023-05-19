-- Create tables for test  ETL
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

DROP TABLE `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`;

-- Create table for Stage 1
CREATE TABLE IF NOT EXISTS `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`
(
	row_id			STRING NOT NULL,
	row_hash		BYTES(32) NOT NULL,
	card_id			STRING NOT NULL,
	brand			STRING NOT NULL,
	model			STRING NOT NULL,
	year			INT64 NOT NULL,
	price			INT64 NOT NULL,
	transmission 	STRING NOT NULL,
	mileage			INT64 NOT NULL,
	body			STRING NOT NULL,
	engine_vol		INT64 NOT NULL,
	fuel 			STRING NOT NULL,
	exchange		STRING NOT NULL,
	top				STRING NOT NULL,
	vin				STRING NOT NULL,
	crahed			STRING NOT NULL,
	for_spare		STRING NOT NULL,
	city			STRING NOT NULL,
	region			STRING NOT NULL,
	comment			STRING NOT NULL,
	scrap_date		TIMESTAMP NOT NULL,
	modified_date	TIMESTAMP NOT NULL,
	deleted			STRING NOT NULL,
	PRIMARY KEY(row_id) NOT ENFORCED
);

--audit_log
CREATE TABLE IF NOT EXISTS `paid-project-346208`.`meta_ds`.`audit_log`
(
	id			STRING NOT NULL,
	process		STRING NOT NULL,
	start_ts	TIMESTAMP NOT NULL,
	stop_ts		TIMESTAMP,
	message		STRING
);

WITH t1 AS
(
	SELECT
		card_id,
		title,
		price_primary,
		location,
		labels,
		comment,
		description,
		exchange,
		scrap_date
		ROW_NUMBER() OVER(PARTITION BY card_id ORDER BY scrap_date DESC) AS cnt
	FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
)
SELECT card_id, title, description, scrap_date
FROM t1
WHERE t1.cnt = 1;


CREATE OR REPLACE PROCEDURE `paid-project-346208`.`car_ads_ds_staging_test`.usp_landing_staging1_av_by_card_tokenized_full_load()
BEGIN 
	-- start audit
	DECLARE id_row STRING;
	DECLARE truncate_row_count INT64;
	DECLARE insert_row_count INT64;

	SET id_row = GENERATE_UUID();
	--create log record
	INSERT INTO `paid-project-346208`.`meta_ds`.`audit_log`(id, process, start_ts)
		VALUES(id_row, "usp_landing_staging1_av_by_card_tokenized_full_load", CURRENT_TIMESTAMP());

	--get quantity of rows which will be truncated
	
	SET truncate_row_count = (SELECT COUNT(card_id) FROM `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`);

	TRUNCATE TABLE `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`;

	INSERT INTO `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`
	WITH t1 AS
	(
		SELECT
			card_id,
			title,
			price_secondary,
			location,
			labels,
			comment,
			description,
			exchange,
			scrap_date,
			ROW_NUMBER() OVER(
			PARTITION BY 
				card_id,
				title,
				price_secondary,
				location,
				labels,
				comment,
				description,
				exchange 
			ORDER BY scrap_date ASC
			) AS cnt
		FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
	)
	SELECT
		GENERATE_UUID() AS row_id,
		SHA256(CONCAT(COALESCE(title, ""),
					COALESCE(price_secondary, ""),
					COALESCE(location, ""),
					COALESCE(labels, ""),
					COALESCE(comment, ""),
					COALESCE(description, ""),
					COALESCE(exchange, ""))
				) AS row_hash,
		CAST(card_id AS STRING) AS card_id,
		CASE
			WHEN split(title, ' ')[1] LIKE 'Lada' THEN 'Lada (ВАЗ)'
			WHEN split(title, ' ')[1] LIKE 'Alfa' THEN 'Alfa Romeo'
			WHEN split(title, ' ')[1] LIKE 'Dongfeng' THEN 'Dongfeng Honda'
			WHEN split(title, ' ')[1] LIKE 'Great' THEN 'Great Wall'
			WHEN split(title, ' ')[1] LIKE 'Iran' THEN 'Iran Khodro'
			WHEN split(title, ' ')[1] LIKE 'Land' THEN 'Land Rover'
			ELSE split(title, ' ')[1]
		END AS brand,
		CASE
			WHEN split(title, ' ')[1] LIKE 'Lada' THEN REGEXP_EXTRACT(title, r'Продажа Lada \(ВАЗ\) (.+)[,]')
			WHEN split(title, ' ')[1] LIKE 'Alfa' THEN REGEXP_EXTRACT(title, r'Продажа Alfa Romeo (.+)[,]')
			WHEN split(title, ' ')[1] LIKE 'Dongfeng' THEN REGEXP_EXTRACT(title, r'Продажа Dongfeng Honda (.+)[,]')
			WHEN split(title, ' ')[1] LIKE 'Great' THEN REGEXP_EXTRACT(title, r'Продажа Great Wall (.+)[,]')
			WHEN split(title, ' ')[1] LIKE 'Iran' THEN REGEXP_EXTRACT(title, r'Продажа Iran Khodro (.+)[,]')
			WHEN split(title, ' ')[1] LIKE 'Land' THEN REGEXP_EXTRACT(title, r'Продажа Land Rover (.+)[,]')
			ELSE REGEXP_EXTRACT(title, r'Продажа [a-zA-Zа-яёА-ЯЁ-]+ (.+)[,]')
		END AS model,
		CAST(REGEXP_EXTRACT(description, r'^(\d{4}) г.,') AS INT) AS year,
		CAST(REGEXP_EXTRACT(REPLACE(price_secondary, ' ', ''), r'≈(\d+)\$') AS INT) AS price,
		REGEXP_EXTRACT(description, r',\s(\S+),.+') AS transmission,
		CAST(REPLACE(REGEXP_EXTRACT(description, r',\s(\d+[ ]?\d*) км'), " ", "") AS INT) AS mileage,
		REGEXP_EXTRACT(description, r'\| ([А-Яа-я0-9. ]+)') AS body,
		CAST(REGEXP_EXTRACT(REPLACE(split(description, ',')[2], '.', ''), r'[0-9]+') AS INT) * 100 AS engine_vol,
		CASE 
			WHEN INSTR(description, 'Запас хода') <> 0 THEN 'Electric'
			ELSE split(description, ',')[3]	
		END AS fuel,
		CASE 
			WHEN INSTR(exchange, 'Возможен обмен') <> 0 THEN 'Y'
			WHEN INSTR(exchange, 'Возможен обмен с моей доплатой') <> 0 THEN 'Y'
			WHEN INSTR(exchange, 'Возможен обмен с вашей доплатой') <> 0 THEN 'Y'
			WHEN INSTR(exchange, 'Обмен не интересует') <> 0 THEN 'N'
			ELSE 'N'
		END AS exchange,
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
		END AS comment,
		scrap_date AS scrap_date,
		scrap_date AS modified_date,
		"N" AS deleted
	FROM t1
	WHERE t1.cnt = 1; --delete duplicates
	
	--get quantity of rows which have been inserted
	SET insert_row_count = (SELECT COUNT(card_id) FROM `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized`);
	-- update audit record
	UPDATE `paid-project-346208`.`meta_ds`.`audit_log`
	SET stop_ts = CURRENT_TIMESTAMP(),
		message = CONCAT("deleted:0;truncated:", CAST(truncate_row_count AS STRING), ";inserted:", CAST(insert_row_count AS STRING),";updated:0")
	WHERE id = id_row;
	
END;



CALL `paid-project-346208`.`car_ads_ds_staging_test`.usp_landing_staging1_av_by_card_tokenized_full_load();

SELECT SHA256(CONCAT(t1.title, t1.price_secondary, t1.location, t1.labels, t1.comment, t1.description, t1.exchange)) AS row_hash
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300` AS t1;

SELECT t1.title, t1.price_secondary, t1.location, t1.labels, t1.comment, t1.description, t1.exchange
FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300` AS t1;

UPDATE `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
SET price_secondary = "≈ 153 $"
WHERE card_id = 104316191;

UPDATE `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
SET scrap_date = CAST("2023-04-18 21:43:33.000" AS TIMESTAMP)
WHERE card_id = 104316191;


CREATE OR REPLACE PROCEDURE `paid-project-346208`.`car_ads_ds_staging_test`.usp_landing_staging1_av_by_card_tokenized_incremental_load()
BEGIN
	DECLARE id_row STRING;
	DECLARE insert_row_count INT64;

	SET id_row = GENERATE_UUID();

	--create log record
	INSERT INTO `paid-project-346208`.`meta_ds`.`audit_log`(id, process, start_ts)
		VALUES(id_row, "usp_landing_staging1_av_by_card_tokenized_full_load", CURRENT_TIMESTAMP());
	
	MERGE `paid-project-346208`.`car_ads_ds_staging_test`.`cars_av_by_card_tokenized` AS T
	USING
	(
		SELECT
			card_id,
			title,
			price_secondary,
			location,
			labels,
			comment,
			description,
			exchange,
			scrap_date,
			ROW_NUMBER() OVER(
			PARTITION BY 
				card_id,
				title,
				price_secondary,
				location,
				labels,
				comment,
				description,
				exchange 
			ORDER BY scrap_date ASC
			) AS cnt
		FROM `paid-project-346208`.`car_ads_ds_landing`.`lnd_cars-av-by_card_300`
		WHERE cnt = 1
	) AS S
	ON 
END;  