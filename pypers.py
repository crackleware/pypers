import weakref
import cPickle as pickle
import uuid

import bsddb3

main_db = None

class Persistent:
    def __init__(self, _key=None, _db=None):
        if _db is None: _db = main_db
        self._db = _db
        if _key is None: _key = str(uuid.uuid4())
        self._key = _key
        if self._key not in self._db:
            self._db[self._key] = self
        self._db.key2obj[self._key] = self

    def __getitem__(self, attr):
        return self.__getattr__(attr)
    def __setitem__(self, attr, value):
        return self.__setattr__(attr, value)
    def __delitem__(self, attr):
        return self.__delattr__(attr)

    def _attr_key(self, attr):
        return self._key+'_'+repr(attr)
    
    def __setattr__(self, attr, value):
        if str(attr).startswith('_'):
            self.__dict__[attr] = value
        else:
            k = self._attr_key(attr)
            self._db[k] = value

    def __getattr__(self, attr):
        #print 'getattr', '0x%X' % id(self), attr
        if attr in self.__dict__:
            return self.__dict__[attr]
        elif not str(attr).startswith('_'):
            k = self._attr_key(attr)
            if k in self._db:
                return self._db[k]
            else:
                o = Persistent(_db=self._db)
                self.__setattr__(attr, o)
                return o
        raise AttributeError(attr)

    def __delattr__(self, attr):
        if attr in self.__dict__:
            del self.__dict__[attr]
        elif not str(attr).startswith('_'):
            k = self._attr_key(attr)
            del self._db[k]
        else:
            raise AttributeError(attr)

    def __getstate__(self):
        #print 'getstate', self
        return {'_key': self._key}
    def __setstate__(self, state):
        self.__dict__ = state
        #print 'setstate', self, state

    def __repr__(self):
        return '%s(_key=%r)' % (self.__class__.__name__, self._key)

    def iterkeys(self):
        k = self._db.db.set_location(self._key)
        while 1:
            if k[0] != self._key:
                if k[0].startswith(self._key+'_'):
                    s = k[0][len(self._key+'_'):]
                    if 1: # temp fix
                        if s.startswith('Persistent@'):
                            p1 = s.find('@')
                            p2 = s.find('(')
                            s = s[0:p1]+s[p2:]
                        if s.startswith('Persistent('):
                            s = s.replace('Persistent(_key=', 'Persistent(_db=self._db, _key=')
                            s = s.replace('Persistent(key=', 'Persistent(_db=self._db, _key=')
                    attr = eval(s)
                    yield attr
                else:
                    break
            try:
                k = self._db.db.set_location(k[0])
                k = self._db.db.next()
            except:
                break

    def keys(self):
        return list(self.iterkeys())
        
    def iteritems(self):
        for k in self.iterkeys():
            yield (k, self[k])

    def __iter__(self):
        for _, v in self.iteritems():
            yield v
            
class DB:
    def __init__(self, filename, flags='c'):
        self.db = bsddb3.btopen(filename, flags)
        self.key2obj = weakref.WeakValueDictionary()

    def close(self):
        self.db.sync()
        self.db.close()

    def get(self, key):
        return Persistent(key, self)
        
    def __getitem__(self, key):
        if key in self.key2obj:
            return self.key2obj[key]
        else:
            v = pickle.loads(self.db[key])
            if isinstance(v, Persistent):
                if v._key in self.key2obj:
                    return self.key2obj[v._key]
                else:
                    v._db = self
                    self.key2obj[v._key] = v
                    return v
            else:
                return v

    def __setitem__(self, key, value):
        v = pickle.dumps(value)
        #print 'DB setitem', (key, v)
        self.db[key] = v

    def __delitem__(self, key):
        del self.db[key]

    def __contains__(self, key):
        return key in self.db

if __name__ == '__main__':
    # tests
    import sys
    import time
    from pprint import pprint

    def print_db(db):
        k = db.db.first()
        while 1:
            print k
            try:
                k = db.db.next()
            except:
                break
        
    if sys.argv[1] == 'test-1':
        db = DB('test.db')
        root = db.get('root')
        root.attr1 = 123
        assert root.attr1 == 123
        root.attr2 = 'abc'
        assert root.attr2 == 'abc'
        root[333] = 'ttt'
        assert root[333] == 'ttt'
        assert root.keys() == ['attr1', 'attr2', 333]
        root.attr3 = root.attr4
        assert root.attr3 == root.attr4
        #for v in root: print v
        print_db(db)
        
    elif sys.argv[1] == 'test-2':
        db = DB('test.db')
        root = db.get('root')
        assert root.attr1 == 123
        assert root.attr2 == 'abc'
        assert root[333] == 'ttt'
        assert root.attr3 == root.attr4
        print_db(db)
        
    elif sys.argv[1] == 'test-3':
        db = DB('test.db')
        root = db.get('root')
        
        n = 100000

        t1 = time.time()
        for i in xrange(n):
            root[i] = i
        t2 = time.time()
        print 'set: dt: %g s (%g set/s)' % (t2-t1, n/(t2-t1))

        t1 = time.time()
        for i in xrange(n):
            assert root[i] != None
        t2 = time.time()
        print 'get: dt: %g s (%g get/s)' % (t2-t1, n/(t2-t1))
        
        print 'len(db.db):', len(db.db)
        print 'len(db.key2obj):', len(db.key2obj)

