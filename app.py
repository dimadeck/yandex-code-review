from flask import Flask, abort, request, jsonify
import elasticsearch as ES

from validate import validate_args

app = Flask(__name__)


@app.route('/')
def index():
    # todo CR: Подобный код необходимо убрать, тем более он на главной странице приложения. Хорошим решением будет
    #  сделать редирект на /api/movies
    return 'worked'


@app.route('/api/movies/')
def movie_list():
    validate = validate_args(request.args)
    # todo CR: Для получения значения словаря рекомендуется использовать 'validate.get('success')'
    if not validate['success']:
        return abort(422)
    # todo CR: В данном случае лучше записать в одну строку - негативно на читаемость это не повлияет
    defaults = {
        'limit': 50,
        'page': 1,
        'sort': 'id',
        'sort_order': 'asc'
    }

    # todo CR: Бесполезный комментарий
    # Тут уже валидно все
    # todo CR: Лучше сделать 'for param, value in request.args:', тогда присвоение будет лаконичнее и
    #  исключит ошибку неправильной передачи ключа: 'defaults[param] = value'
    for param in request.args.keys():
        defaults[param] = request.args.get(param)

    # todo CR: Нагруженный код, допустимо, но может негативно влиять на читаемость
    # Уходит в тело запроса. Если запрос не пустой - мультисерч, если пустой - выдает все фильмы
    body = {
        "query": {
            "multi_match": {
                # todo CR: значение лучше получать через метод словаря get
                "query": defaults['search'],
                "fields": ["title"]
            }
        }
        # todo CR: значение по-умолчанию в данном случае бесполезно
    } if defaults.get('search', False) else {}

    # todo CR: Можно заполнить одной инструкцией: body['_source'] = {'include': ['id', 'title', 'imdb_rating']}
    body['_source'] = dict()
    body['_source']['include'] = ['id', 'title', 'imdb_rating']

    params = {
        # todo CR: Финальный код нужно чистить от неиспользуемых инструкций
        # '_source': ['id', 'title', 'imdb_rating'],
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }
    # todo CR: Рекомендуется не использовать "жесткую" конфигурацию при инициализации экземляра.
    #  Лучше: задать ее в: шапке модуля, конфигурационном модуле, или через входные аргументы.
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )
    search_res = es_client.search(
        body=body,
        index='movies',
        params=params,
        filter_path=['hits.hits._source']
    )
    es_client.close()

    return jsonify([doc['_source'] for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):
    # todo CR: дублирование данных, еще один аргумент за то, чтобы вынести конфигурацию выше
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )

    if not es_client.ping():
        # todo CR: Не рекомендуется писать подобные инструкции. Лучше использовать систему логирования(DEBUG)
        #  с указанием полезных данных
        print('oh(')

    search_result = es_client.get(index='movies', id=movie_id, ignore=404)

    es_client.close()

    # todo CR: значение лучше получать через метод словаря get
    if search_result['found']:
        return jsonify(search_result['_source'])
    # todo CR: значение лучше получать через метод словаря get

    return abort(404)


if __name__ == "__main__":
    # todo CR: Неплохой код, несколько замечаний, рекомендация к использованию блоков try-except, возврат статусов
    #  лучше через заранее объявленные переменные (например, status_ok = 200)
    app.run(host='0.0.0.0', port=80)
