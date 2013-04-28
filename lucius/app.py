import os

try:
    import simplejson as json
except ImportError:
    import json

import lucene
lucene.initVM()

from indexer import GetIndexers, start_indexer

from flask import g, request, Response
from flask import Flask
app = Flask(__name__)

@app.route("/")
def index():
    return json.dumps({"status": "running",
        "info": "Couchdb pylucene indexer"})

@app.route("/db/<database>/<view>/<index>")
def search(database, view, index):
    name = "%s/%s/%s" % (database, view, index)
    query = request.args.get("q", "")

    include_docs = request.args.get("include_docs", False)
    include_docs == "true" and True or False

    limit = request.args.get("limit", "25")
    limit = limit.isdigit() and int(limit) or 0

    skip = request.args.get("skip", "0")
    skip = skip.isdigit() and int(skip) or 0

    try:
        indexer = g.indexers[name]
    except KeyError:
        return json.dumps({"Error": "indexer not found"})

    rows= []
    to_fetch = []
    hits = indexer.search(query, limit=limit+skip)
    for num, hit in enumerate(hits):
        if num < skip:
            continue

        if include_docs:
            to_fetch.append(hit.dict()["_id"])
        else:
            row = {}
            for key, value in hit.dict().iteritems():
                if key.startswith("_") and not key == '_id':
                    continue
                row[key] = value
            rows.append(row)

        if include_docs:
            rows = indexer.get_docs(to_fetch)

    ret = {"rows": rows, "limit": limit, "skip": skip, "total_rows": len(hits)}
    json_data = json.dumps(ret)

    response = Response(json_data, content_type="application/json")
    return response

@app.route("/add/<database>")
def add_database(database):
    if not start_indexer(database):
        return json.dumps({"status": "error",
            "message": "already indexing %s" % database})
    return json.dumps({"status": "ok", "message": "indexing %s" % database})

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

def start_app():
    try:
        import config
    except ImportError:
        config = None
    configure_app(app, config)
    app.run(port=config.PORT)

if __name__ == "__main__":
    start_app()
