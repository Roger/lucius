import time
import thread
import hashlib
import collections

from flask import current_app, g

from .utils import LuceneDocument, get_field, get_designs, _print_
from lupyne import engine
from lupyne.engine import indexers

from RestrictedPython import compile_restricted
from RestrictedPython.Guards import safe_builtins

import couchdb

import lucene

from werkzeug.contrib.cache import SimpleCache
cache = SimpleCache()

db_indexers = {}
threads = {}

class GetIndexers(dict):
    def __getitem__(self, key):
        try:
            indexer = super(GetIndexers, self).__getitem__(key)
            self[key] = indexer
        except KeyError:
            indexer = db_indexers.get(key, None)
            if not indexer:
                raise KeyError, "Database not found"
            lucene.getVMEnv().attachCurrentThread()
        return indexer

class CustomIndexer(engine.Indexer):
    def commit(self, seq, merge=False, **caches):
        """Commit writes and :meth:`refresh` searcher.

        :param merge: merge segments with deletes, or optionally specify maximum number of segments
        """
        hash_map = lucene.HashMap().of_(lucene.String, lucene.String)
        hash_map.put("update_seq", seq)

        engine.IndexWriter.commit(self, hash_map)
        if merge:
            if isinstance(merge, bool):
                self.forceMergeDeletes()
            else:
                self.forceMerge(merge)
            engine.IndexWriter.commit(self, hash_map)
        self.refresh(**caches)

    def get_sequence(self):
        user_data = self.indexReader.getIndexCommit().getUserData()
        update_seq = user_data.get("update_seq")
        return update_seq and int(update_seq) or 0

class DBIndexer(object):
    def __init__(self, db, indexer_name, func, index_path="/tmp/couchdb_index",
            restrict=True):
        self.db = db
        self.restrict = restrict
        self.func = self.compile_indexer(indexer_name, func)

        func_digest = hashlib.md5(func).hexdigest()
        indexer_dir = "%s/%s-%s" % (index_path, indexer_name, func_digest)
        self.indexer = CustomIndexer(indexer_dir)
        self.dirty = False
        self.update_seq = self.indexer.get_sequence()
        self.last_update = time.time()

        self.views = []

    def close(self):
        self.indexer.close()

    def info(self):
        """
        Returns indexer info
        """
        info = {
            #"uuid": state.getUuid(),
            #"digest": state.getDigest(),
            #"etag": self.indexer.etag,
            "update_seq": self.update_seq,
            "last_modified": self.indexer.lastModified(self.indexer.directory),
            "fields": list(self.indexer.names()),
            "optimized": self.indexer.optimized,
            "doc_count": self.indexer.numDocs(),
            "current": self.indexer.isCurrent(),
            "del_doc": self.indexer.indexReader.numDeletedDocs(),
            "ref_count": self.indexer.getRefCount(),
            }
        return info

    def compile_indexer(self, indexer_name, func):
        safe_globals = dict(LuceneDocument=LuceneDocument)
        safe_locals = {}
        if self.restrict:
            safe_globals.update(dict(
                            _print_=_print_(indexer_name),
                            _getattr_=getattr,
                            _getitem_=lambda o, k: o[k],
                            __builtins__=safe_builtins)
                            )
            obj = compile_restricted(func, "<string>", "exec")
        else:
            obj = compile(func, "<string>", "exec")
        eval(obj, safe_globals, safe_locals)
        index_doc = safe_locals.get("fun")
        return index_doc

    def update(self, row, ignore_seq=False):
        self.set_doc_cache(row)
        self.update_related(row["id"])

        try:
            luc_docs = self.func(row["doc"])
        except Exception, error:
            print "Invalid indexer"
            print error
            luc_docs = []

        if not luc_docs:
            self.dirty = True
            if not ignore_seq:
                self.update_seq = row["seq"]
            return
        if not isinstance(luc_docs, collections.Iterable):
            luc_docs = [luc_docs]

        for luc_doc in luc_docs:
            # search related documents and add field to the index
            related_items = set()
            for related_field in luc_doc._related_fields:
                name, docid, doc_field, field_type, params = related_field
                related_items.add(docid)
                doc = self.get_doc(docid)
                if not doc:
                    print "Doc Not Found: '%s' for related '%s'" % (docid, name)
                    continue

                field_value = get_field(doc, doc_field)
                if not field_value:
                    print "Field Not Found: %s" % doc_field
                    continue
                luc_doc.add_field(name, field_value, field_type, **params)

            for related_item in related_items:
                luc_doc.add_field("_related_item", related_item)

            luc_doc.add_field("_id", row["id"], store=True)
            #print "Updating Document"
            self.dirty = True
            if not ignore_seq:
                self.update_seq = row["seq"]

            self.indexer.updateDocument(indexers.index.Term("_id", row["id"]),
                    luc_doc)

        if time.time() - self.last_update > 15:
            self.commit()

    def get_doc(self, docid):
        cache_key = "docs/%s" % docid
        doc = cache.get(cache_key)
        if not doc:
            doc = self.db.get(docid)
            if doc:
                cache.set(cache_key, doc, timeout=60)
        return doc

    def set_doc_cache(self, row):
        cache_key = "docs/%s" % row["id"]
        cache.set(cache_key, row["doc"], timeout=60)

    def update_related(self, id):
        items = [hit.dict()["_id"] \
                for hit in self.indexer.search("_related_item:%s" % id)]
        if not items:
            return

        start = time.time()
        print "Start updating related"

        view = self.db.view("_all_docs", keys=items, include_docs=True)
        print "Rows Len", len(view)
        print id, len(items)
        for row in view:
            if row["doc"]:
                self.update(row, ignore_seq=True)
        print "End updating related", time.time() - start, len(items)

    def delete(self, row):
        self.indexer.deleteDocuments(indexers.index.Term("_id", row["id"]))
        self.dirty = True
        self.update_seq = row["seq"]
        self.update_related(row["id"])
        self.commit()

    def commit(self):
        if self.dirty:
            print "UPDATE SEQ", self.update_seq
            print "Commiting index changes"
            print
            self.indexer.commit(str(self.update_seq))
            self.dirty = False
        self.last_update = time.time()

    def search(self, query, limit=25, **kwargs):
        return self.indexer.search(query, count=limit, **kwargs)

    def get_docs(self, ids):
        view = self.db.view("_all_docs", keys=ids, include_docs=True)
        rows = [row["doc"] for row in view if row["doc"]]
        return rows

def _run_indexer(config, db_name):
    local_indexers = []
    server = config["COUCHDB_SERVER"]
    database = couchdb.Database("%s/%s/" % (server, db_name))

    # set the current sequence before design docs retrival
    design_doc_seq = database.info()["update_seq"]
    update_sequences = []
    for row in get_designs(db_name, config):
        doc = row["doc"]
        ft_view = doc.get("ft", None)
        if not ft_view:
            continue
        for key, value in ft_view.iteritems():
            view_name = doc["_id"].split("_design/", 1)[1] + "/" + key
            indexer_name = "%s/%s" % (db_name, view_name)
            indexer = DBIndexer(database, indexer_name, value["index"],
                    config["INDEX_PATH"], config["RESTRICT"])
            local_indexers.append(indexer)
            db_indexers[indexer_name] = indexer
            update_sequences.append(indexer.update_seq)

    if not update_sequences:
        print "Invalid Database"
        return

    update_seq = min(update_sequences)
    max_update_seq = max(update_sequences)
    if max_update_seq > design_doc_seq:
        design_doc_seq = max_update_seq

    del update_sequences

    for row in database.changes(feed="continuous", heartbeat=10000,
             include_docs=True, yield_beats=True, since=update_seq):
        last_seq = row.get("seq", None)

        # if a design view is updated/created restart the indexer
        # and close current indexes
        if row.get("id", "").startswith("_design") and last_seq > design_doc_seq:
            [indexer.close() for indexer in local_indexers]
            return True

        for indexer in local_indexers:
            # heartbeat
            if not row:
                indexer.commit()
                continue
            if last_seq < indexer.update_seq:
                continue

            if row["doc"].get("_deleted") == True:
                indexer.delete(row)
            else:
                indexer.update(row)

def _start_indexer(config, db_name):
    jcc_evn = lucene.getVMEnv()
    jcc_evn.attachCurrentThread()

    while True:
        restart = _run_indexer(config, db_name)
        if not restart:
            print "Exit db indexer %s" % db_name
            break
        print "Restarted db indexer %s" % db_name

def start_indexer(database, config=None):
    if database in threads:
        return False
    config = config or current_app.config
    t = thread.start_new_thread(_start_indexer, (config, database))
    threads[database] = t
    return True

def get_indexer(database, view, index, start=True):
    # start indexing if not indexing already
    not_exists = start_indexer(database)
    name = "%s/%s/%s" % (database, view, index)

    count = 0
    while True:
        count += 1
        try:
            return g.indexers[name]
        except KeyError:
            if start and not_exists and count <= 5:
                time.sleep(.1)
                continue
            return
