import thread
import collections

from flask import current_app

from utils import LuceneDocument
from lupyne import engine
from lupyne.engine import indexers

from RestrictedPython import compile_restricted
from RestrictedPython.Guards import safe_builtins

import couchdb

import lucene

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
    def __init__(self, db, indexer_name, func, index_path="/tmp/couchdb_index"):
        self.db = db
        self.func = func
        indexer_dir = "%s/%s" % (index_path, indexer_name)
        self.indexer = CustomIndexer(indexer_dir)
        self.dirty = False
        self.update_seq = self.indexer.get_sequence()

    def update(self, row):
        #luc_docs = index_doc(row["doc"])
        safe_globals = dict(LuceneDocument=LuceneDocument,
                            _getattr_=getattr,
                            _getitem_=lambda o, k: o[k],
                            __builtins__=safe_builtins)
        safe_locals = {}
        obj = compile_restricted(self.func, "<string>", "exec")
        eval(obj, safe_globals, safe_locals)
        index_doc = safe_locals.get("fun")
        try:
            luc_docs = index_doc(row["doc"])
        except Exception, error:
            print "Invalid indexer"
            print error

        if not luc_docs:
            return
        if not isinstance(luc_docs, collections.Iterable):
            luc_docs = [luc_docs]

        for luc_doc in luc_docs:
            luc_doc.add_field("_id", row["id"], store=True)
            self.indexer.updateDocument(indexers.index.Term("_id", row["id"]), luc_doc)
            print "Updating Document"
            self.dirty = True
            self.update_seq = row["seq"]

    def delete(self, row):
        self.indexer.deleteDocuments(indexers.index.Term("_id", row["id"]))
        self.dirty = True
        self.update_seq = row["seq"]
        self.commit()

    def commit(self):
        if self.dirty:
            print "UPDATE SEQ", self.update_seq
            print "Commiting index changes"
            self.indexer.commit(str(self.update_seq))
            self.dirty = False

    def search(self, query, limit=25):
        return self.indexer.search(query, count=limit)

    def get_docs(self, ids):
        view = self.db.view("_all_docs", keys=ids, include_docs=True)
        rows = [row["doc"] for row in view if row["doc"]]
        return rows


def _start_indexer(config, database):
    jcc_evn = lucene.getVMEnv()
    jcc_evn.attachCurrentThread()

    local_indexers = []

    server = config["COUCHDB_SERVER"]
    db = couchdb.Database("%s/%s/" % (server, database))

    view = db.view("_all_docs", startkey="_design", endkey="_design0",
            include_docs=True)
    update_sequences = []
    for row in view:
        doc = row["doc"]
        ft_view = doc.get("ft", None)
        if not ft_view:
            continue
        for key, value in ft_view.iteritems():
            view_name = doc["_id"].split("_design/", 1)[1] + "/" + key
            indexer_name = "%s/%s" % (database, view_name)
            indexer = DBIndexer(db, indexer_name, value["index"],
                    config["INDEX_PATH"])
            local_indexers.append(indexer)
            db_indexers[indexer_name] = indexer
            update_sequences.append(indexer.update_seq)
    update_seq = min(update_sequences)
    del update_sequences

    for row in db.changes(feed="continuous", heartbeat=10000, include_docs=True,
            yield_beats=True, since=update_seq):
        last_seq = row.get("seq", None)
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

def start_indexer(database):
    if database in threads:
        return False
    config = current_app.config
    t = thread.start_new_thread(_start_indexer, (config, database))
    threads[database] = t
    return True
