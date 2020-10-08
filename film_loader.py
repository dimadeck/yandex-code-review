import sqlite3
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def extract():
    """
    # todo CR: Если используются комментарии на русском, то и docstring можно писать на русском для большей
        информативности и лучшего понимания между коллегами.
        Также хорошей практикой использования docstring является пояснение с какими данными работает метод,
        в данном случае: пояснение используемых полей в таблице.
    extract data from sql-db
    # todo CR: Так как метод возвращает данные, то писать пустой return неправильно.
        Вариант 1. Пояснить что возвращается (actors, writers, raw_data)
        Вариант 2. Не писать блок return в данном docstring
    :return:
    """
    # todo CR: Для решения одной задачи допускается в теле метода использовать жесткие имена файлов,
    #  но хорошей практикой будет вынести имя базы данных вынести в аргументы: 'def extract(db_name="db.sqlite")' и
    #  использовать 'sqlite3.connect(db_name)'. Значение по-умолчанию можно не задавать.
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()

    # todo CR: В комментариях не рекомендуется(часто не допускается) писать подобные мысли. Они не поясняют работу кода,
    #  если код не устраивает - создать задачу на рефакторинг или не писать ничего.
    # Наверняка это пилится в один sql - запрос, но мне как-то лениво)

    # todo CR: Sql запрос выглядит громоздко. Если есть возможность сделать более простым и читаемым - лучше сделать
    # Получаем все поля для индекса, кроме списка актеров и сценаристов, для них только id
    cursor.execute("""
        select id, imdb_rating, genre, title, plot, director,
        -- comma-separated actor_id's
        (
            select GROUP_CONCAT(actor_id) from
            (
                select actor_id
                from movie_actors
                where movie_id = movies.id
            )
        ),

        max(writer, writers)
        from movies
    """)

    raw_data = cursor.fetchall()
    # todo CR: Не рекомендуется оставлять закомментированный код в готовом продукте
    # cursor.execute('pragma table_info(movies)')
    # pprint(cursor.fetchall())

    # Нужны для соответсвия идентификатора и человекочитаемого названия
    actors = {row[0]: row[1] for row in cursor.execute('select * from actors where name != "N/A"')}
    writers = {row[0]: row[1] for row in cursor.execute('select * from writers where name != "N/A"')}

    return actors, writers, raw_data


# todo CR: В данном случае лучше не использовать именование аргументов начиная с '__'. Именование данным способом
#  применяется для инкапсуляции в классах.
def transform(__actors, __writers, __raw_data):
    """
    # todo CR: Не желательно оставлять незаполненный docstring. Либо не писать docstring вообще,
        либо заполнить минимум - Типы входных данных и блок возврата (с указанием типа)
    :param __actors:
    :param __writers:
    :param __raw_data:
    :return:
    """
    documents_list = []
    for movie_info in __raw_data:
        # Разыменование списка
        movie_id, imdb_rating, genre, title, description, director, raw_actors, raw_writers = movie_info
        # todo CR: В данном случае, условие лучше поменять на 'if raw_writers.startswith('['):'
        if raw_writers[0] == '[':
            parsed = json.loads(raw_writers)
            new_writers = ','.join([writer_row['id'] for writer_row in parsed])
        else:
            new_writers = raw_writers

        writers_list = [(writer_id, __writers.get(writer_id)) for writer_id in new_writers.split(',')]
        actors_list = [(actor_id, __actors.get(int(actor_id))) for actor_id in raw_actors.split(',')]

        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": imdb_rating,
            "genre": genre.split(', '),
            "title": title,
            "description": description,
            "director": director,
            "actors": [
                {
                    "id": actor[0],
                    "name": actor[1]
                }
                for actor in set(actors_list) if actor[1]
            ],
            "writers": [
                {
                    "id": writer[0],
                    "name": writer[1]
                }
                for writer in set(writers_list) if writer[1]
            ]
        }

        for key in document.keys():
            if document[key] == 'N/A':
                # todo CR: Недопустимый комментарий
                # print('hehe')
                document[key] = None

        document['actors_names'] = ", ".join([actor["name"] for actor in document['actors'] if actor]) or None
        document['writers_names'] = ", ".join([writer["name"] for writer in document['writers'] if writer]) or None
        # todo CR: инструкции import должны быть в шапке модуля, за исключением особых случаев. Это не тот случай.
        import pprint
        pprint.pprint(document)

        documents_list.append(document)

    return documents_list


# todo CR: В данном случае именование функции может сбить с толку - по факту тут сохранение данных в Elastic.
def load(acts):
    """
    # todo CR: Незаполненный docstring.
    :param acts:
    :return:
    """
    # todo CR: Рекомендуется не использовать "жесткую" конфигурацию при инициализации экземляра.
    #  Лучше: 'def load(acts, elastic_config)', а elastic_config задать в: 1) шапка модуля, 2) конфигурационный модуль,
    #  3) через входные аргументы.
    es = Elasticsearch([{'host': '192.168.1.252', 'port': 9200}])
    bulk(es, acts)
    # todo CR: Возврат в данном случае не имеет смысла
    return True


if __name__ == '__main__':
    # todo CR: В целом хороший код, удобночитаемый. Отсутствуют блоки try, для отлова ошибок -
    #  например, отсутствие соединения с Elasticsearch, или данные в таблице будут некорректными.
    #  Лучше добавить блок try-except в циклы, если где-то ошибка - это не повлияет на выполнение программы в целом,
    #  основные(правильные) данные будут перенесены в БД.
    load(transform(*extract()))
