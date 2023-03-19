# instagramstories
Automatization of downloading Stories from Instagram accounts

InstaStoryParser

Установка скрипта, просмотр логов

github link: https://github.com/chimchimster/instagramstories

Запуск скрипта на сервере:

Клонируем скрипт с гита:
Сперва клонируем репозиторий с гита. Открываем рабочую директорию и прописываем: 

(sudo) git clone https://github.com/chimchimster/instagramstories.git

Монтируем Докер образ:
Скрипт работает в связке с контейнером Докер. Монтируем образ находясь в директории, где лежит Dockerfile:

(sudo) docker build -t <название образа - придумай сам> .  - точка в конце это как раз директория где и находится наш докерфайл.

Запуск образа:
(sudo) docker run <название вашего образа>
(на этом моменте скрит уже работает)

Где посмотреть логи:
Теперь заглянем внутрь образа. Для этого узнаем ID образа с помощью команды:

(sudo) docker ps -l

Среди смонтированных образов находим по названию тот, что был смонтирован нами ранее. Копируем ID

Теперь идем внутрь контейнера:

(sudo) docker exec -it <ID контейнера> bash

Чтобы посмотреть логи внутри контейнера прописываем следующие команды:

cd instagramstories
cd logs
cd history
ls  - это нужно чтобы понять в каком файле содержатся текущие логи. Файлы формируются по дате. Для каждого нового дня создается новый файл с логами. Находим нужный по дню файл и смотрим его с помощью
cat <название файла>

Работа скрипта

Базы данных ClickHouse and MariaDB

Скрипт работает в связке с 3мя базами данных (одна на основе МарияДБ, дургая КликХаус).

Модуль работы с базами данных находится:

app/instagramstories/db_init

Внутри данной директории лежит файл database.py. Он отвечает за коннект к каждой из разновидностей данных СУБД. В файле лежат 2 класса:

MariaDataBase

и

ClickHouseDatabase

Начнем с класса MariaDataBase.
Подключение к базе данных в данном классе реализовано с помощью декоратора. Сразу о грустном…  Данная форма проектирования (по крайней мере без лишних костылей) не позволяет подключить дополнительную базу MariaDB к скрипту. Сделать это конечно можно, если вдруг вам потребуется расширить скрипт и добавить новый коннект то рекомендую воспользоваться наследованием и переопределить методы родительского класса, а также передать доп аргументы в декоратор, т.е. пробросить новый коннект. А еще более костыльный метод - прописать точно такой же декоратор

Данные для подключения к данной базе данных находятся в файле settings.py, в директории app/instagramstories/settings.py. Данные представлены в виде хэш-таблицы - social_services_db. Если у вас возникла проблема с подключением к базе MariaDB - обратитесь к файлу settings.py…. возможно, проблема подключения кроется в неправильных входных данных.

В файле settings.py также содержатся также словари для подключения к базам КликХаус - imas_db и attachments_db.

Документация по работе с классом и всеми его методами можно найти в коде в виде докстрок.

Работа с ClickHouse
Подключение к базе КликХауса происходит намного проще. Чтобы инициировать подключение необходимо создать экземпляр класса ClickHouseDatabase и в конструктор передать необходимые параметры (те, что как раз таки и находятся в settings.py - host, port, login, password). При этом автоматически создастся атрибут connection в конструкторе класса Работать с этой БД в этом смысле гораздо проще.

Модуль для скачивания Stories - instaloader_init
Находится данный модуль в директории app/instagramstories/instaloader_init. Внутри диркетории вы найдете файл loader_init.py.
Модуль состоит из двух классов - SignIn и LoadStoriesOfUser. Первый класс позволяет произвести подключение (логин) к аккаунту инстаграмма с помощью которого мы будем просматривать сторис, а впоследствии и скачивать их. Второй класс отвечает за сохранение полученных сторис (если они вообще есть) в отдельную директорию instagramstories/media.

Скрипт использует библиотеку instaloader для загрузки сторис. Библиотека основана на requests и graphql. 

instaloader documentation: https://instaloader.github.io/
instaloader github: https://github.com/instaloader/instaloader

Для полного понимания работы скрипты обратитесь к документации инсталоадера.

Модуль для определения текста на изображениях - imagehandling
Данный модуль основан на библиотеке pytesseract. Находится в директории app/instagramstories/imagehandling.

pytesseract documentation: https://pytesseract.readthedocs.io/en/latest/

Модуль представлен классом ImageHandling. При создании экземпляра класса в конструктор необходимо передать путь до файла, в котором хранится изображение. После чего применить метод extract_text_from_photo и получить нужный результат текста. Я использовал ряд проверок, чтобы убрать лишние символы из полученного текста, а также определить есть ли вообще хоть какой то адекватный текст на изображении. Если его нету, то вернуть строку ‘empty’, которая в дальнейшем будет добавлена в базу данных.

Модуль подключения и работы с яндекс диском - yadisk_handle
Данный модуль находится в директории app/instagramstories/yadisk_handle. Он состоит из двух составных частей. Первая часть это конфигурация яндекс диска. yadisk_conf.py. В конфигурации мы определяем токен, который необходим для удаленной загрузки файлов на яндекс диск. Вторая часть yadisk_module.py сам модуль, который состоит из нескольких функций	основанных на API яндекса и которые создают пустую директорию, публикуют загруженные файлы, а также загружают файлы прямиком в созданную директорию.

В данном модуле, прочем как и во всех остальных все задокументировано в коде в виде док строк. Если вы сталкиваетесь с трудностями, пожалуйста, обратитесь напрямую к коду.

Основной модуль парсинга instastoryparser
Находится в директории app/instagramstories/instastoryparser. Внутри директории находится файл instaStoryParser.py. Это основной модуль используемый для парсинга. Здесь заключена вся логика по сбору информации. 
Внутри данного скрипта вы найдете:
Обращение ко всем связанным и подключенным базам данных, о  которых шла речь ранее;
Настройка модуля yadisk_handle;
Сбор данных и их последующая запись в базу данных;

Модуль включает в себя функции parse_instagram_stories, get_data_from_db, а также chunks_processing. 

parse_instagram_stories включает в себя также несколько функций:
login_handle - функция отвечает аутентификацию, внутри этой функции отрабатывает функция login и функция collect data 
Функция login позволяет аутентифицировать данные аккаунта, с помощью которого мы будем просматривать сторис, а также их скачивать. Как только мы залогинились в игру вступает функция collect_data
Функция collect_data отвечает за сбор и загрузку полученной нами в ходе парсинга информации. Внутри этой функции объявлена переменная data_to_db, которая в свою очередь является словарем со следующей сигнатурой: data_to_db = {‘path_video’: ‘’, ‘path_photo’: ‘’, ‘path_text’}. Также внутри этой функции объявлена переменная collecttion_to_send, которая изначально представлена пустым списком. Внутри данной функции объявлен главный цикл, который проходится по всем аккаунтам инстаграма необходимым для парсинга (for account in instagram_accounts). Здесь в цикле для каждого аккаунта создается экземпляр класса из модуля loader_init - LoadStoriesOfUser и для каждого аккаунта инициируется загрузка сторис. Как только сторис были скачаны они попадают в папку ../media/{имя аккаунта}/stories. Каждая сторис записывается в формате год-месяц-день-время-UTC.{формат либо mp4 либо jpg}. Кроме того скачиваются и json файлы, но они нам не нужны и будут удалены автоматически во время работы скрипта. Как только скрипт собрал по одному аккаунту информацию он загружает файлы на яндекс диск, но перед этим происходит следующее: если сторис не оказалось то скрипт просто переходит к следующему аккаунту. В противном случае, создается пустая папка в яндекс диске. Затем  в нее загружаются все полученные нами файлы, и в коллекцию data_to_db отправляются а) если это видео - публичный url до файла, который можно впоследствии скачать по ссылке б) если это фото, то в коллекцию также отправляется публичная ссылка, но к тому же скрипт попытается стащить с картинки текст и записать его в ту же коллекцию по ключу ‘path_text’. Если текста не будет он запишет туда ‘empty’. На основе полученных данных, мы наполним коллекцию для отправки в базу данных - collection_to_send. Пример коллекции для отправки в базу данных выглядит примерно так: [[1, ‘https://yadisk.ru/video’, ‘empty’, ‘empty’], [2, ‘empty’, ‘https://yadisk.ru/photo’, ‘hello world’], [3, ‘empty’, ‘https://yadisk.ru/photo’, ‘empty’]]. Миграция в базу данных произойдет при каждом третьем пройденном в цикле аккаунте, а также при условии что в коллекции collection_to_send будет содержаться хоть что-то. После чего коллекции data_to_db и collection_to_send будут очищены. В самом конце функции login_handle объявлена чистка субдиректории media связанной с аккаунтом полученным в цикле.
Функция get_data_from_db возвращает данные, которые мы будем использовать в ходе парсинга. А именно, пулл инстаграм аккаунтов, данные для входа в аккаунт, с которого мы будем смотреть сторисы, а также прокси, которые мы будем использовать для работы нашего скрипта. 

Функция chunk_processing - это один из подходов в реализации многопоточности. Получив пулл аккаунтов для парсинга мы делим их на равные части (чанки) и каждую часть запускаем на отдельном потоке. 

Функция start это entrypoint скрипта.

Дополнительно
В директории instagramstories/ также содержится:
 Файл Dockerfile - настройка контейнера;
 main.py  - точка входа в скрипт;
 requirements - все необходимые зависимости;
./logs/logs_config.py - самая стандартная настройка логгера через библиотеку logger.
