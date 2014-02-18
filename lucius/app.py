import os
import time

try:
    import simplejson as json
except ImportError:
    import json

import coucher

import lucene
from lucene import SimpleHTMLFormatter, StringReader
lucene.initVM()

from indexer import GetIndexers, start_indexer, get_indexer

from flask import g, request, Response, render_template
from flask import Flask

from .utils import get_designs
from .fixers import FixCouchdbProxy

app = Flask(__name__, static_url_path="/_utils/static")

@app.route("/")
def index():
    return json.dumps({"status": "running",
        "info": "Couchdb pylucene indexer"})

@app.route("/_utils/info")
def utils_info():
    designs = {}
    for db, design in get_all_designs():
        designs.setdefault(db, [])
        name = "%s/%s" % (db, design)
        started = name in g.indexers

        indexer = None
        info = {}
        try:
            indexer = g.indexers[name]
            info = ["%s: %s" % (key, value) for key, value in indexer.info().iteritems()]
            info = "<br>".join(info)
            started = True
        except KeyError:
            started = False

        designs[db].append({"design": design, "started": started, "info": info})
    return render_template("info.html", designs=designs)

@app.route("/_utils/search")
def utils_search():
    designs = ["%s/%s" % d for d in get_all_designs()]
    return render_template("search.html", designs=designs)

@app.route("/<database>/<view>/<index>/correct")
def correct(database, view, index):
    field = request.args.get("field")
    words = request.args.getlist("word")
    if not field or not words:
        return json.dumps({"Error": "Needs word and field"})

    indexer = get_indexer(database, view, index)
    start = time.time()
    data = {}
    for word in words:
        data[word] = list(indexer.indexer.correct(field, word))

    ret = {"corrections": data,  "fetch_duration": time.time()-start}
    json_data = json.dumps(ret)

    response = Response(json_data, content_type="application/json")
    return response

@app.route("/<database>/<view>/<index>")
def search(database, view, index):

    indexer = get_indexer(database, view, index)
    if not indexer:
        return json.dumps({"Error": "indexer not found"})

    start = time.time()
    query = request.args.get("q", "")
    if not query:
        response = Response(json.dumps(indexer.info()),
                content_type="application/json")
        return response

    include_docs = request.args.get("include_docs", False)
    include_docs = True if include_docs == "true" else False

    limit = request.args.get("limit", "25")
    limit = limit.isdigit() and int(limit) or 0

    skip = request.args.get("skip", "0")
    skip = skip.isdigit() and int(skip) or 0

    rows_by_id = {}
    rows = []
    to_fetch = []
    hits = indexer.search(query, limit=limit+skip, scores=True)
    for num, hit in enumerate(hits):
        if num < skip:
            continue

        _id = hit.dict()["_id"]

        if include_docs:
            to_fetch.append(_id)

        row = {"__lucene__": {}, "fields": {}}
        for key, value in hit.dict().iteritems():
            if key == '_id':
                row["id"] = value
            elif key.startswith("_"):
                row["__lucene__"][key] = value
            else:
                row["fields"][key] = value

        #row["body"] = body
        rows.append(row)
        rows_by_id[_id] = row

    if include_docs:
        for doc in indexer.get_docs(to_fetch):
            rows_by_id[doc["_id"]]["doc"] = doc

    ret = {"rows": rows, "limit": limit, "skip": skip, "total_rows": len(hits),
            "fetch_duration": time.time()-start}
    json_data = json.dumps(ret)

    response = Response(json_data, content_type="application/json")
    return response

def get_all_designs():
    server = coucher.Server(app.config["COUCHDB_SERVER"])
    designs = []
    for database in server:
        if database.name.startswith("_"):
            continue

        for design in get_designs(database):
            doc = design["doc"]
            ft_view = doc.get("ft", None)
            if not ft_view:
                continue

            for key in ft_view:
                view_name = doc["_id"].split("_design/", 1)[1] + "/" + key
                designs.append((database.name, view_name))
    return designs


@app.before_request
def before_request():
    g.indexers = GetIndexers()

def configure_app(app, config=None):
    if os.environ.get("LUCIUS_CONFIG"):
        app.config.from_envvar("LUCIUS_CONFIG")
    elif config:
        app.config.from_object(config)
    else:
        raise Exception, "Needs a config!"

    lucius_prefix = app.config.get("LUCIUS_PREFIX")

    # fix proxy
    app.wsgi_app = FixCouchdbProxy(app.wsgi_app, script_name=lucius_prefix)

def start_app():
    try:
        import config
    except ImportError:
        config = None

    configure_app(app, config)
    config = app.config
    app.run(port=config["PORT"], threaded=True)

if __name__ == "__main__":
    start_app()
