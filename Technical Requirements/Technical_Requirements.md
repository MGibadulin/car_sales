Задача:
консольная утилита должна поддерживать поиск данных и отображение их в окне стандартного вывода по следующим критериям (аргументам командной строки):
—year_from и —year_to - фильтры на год создания (—year_from 2012) default = all
—brand - марка автомобиля (—brand NISSAN) default = all
—model - модель (—model X-TRAIL) default = all
—price_from и —price_to - фильтры на поле price_secondary, т.е. цену в долларах (—price_to 8000) default = all
—transmission - коробка передач (—transmission автомат) default = all
—mileage - максимальный допустимый пробег (—mileage 10000) default = all
—body - тип кузова автомобиля (—body универсал) default = all
—engine_from и —engine_to - объем двигателя ( —engine_from 1600 —engine_to 2000) default = all
—fuel - тип топлива (—fuel дизель) default = all
—exchange - готовность совершить обмен - yes|no (—exchange yes) default = all
—keywords - любой текст, который вы ищете в объявлении, независимые ключевые слова отделены запятыми (—keywords "одни руки, идеальное состояние, фаркоп") default = all
-max_records (default = 20) только значения больше 0. отрицательные значения меняем на дефолт. (added 20230404)

Экстрактим и обрабатываем данные из файлов csv, выгружаем в окно стандартного вывода. Можно легко перенаправить в текстовый файл
pandas - не использовать