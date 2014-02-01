"""
Aaron Berndsen:
a set of classes designed to integrate with the python nltk, but adapted for
very large datasets.  These classes use a Berkeley database as their backend,
 instead of keeping things in memory.

"""
import cPickle as pickle
import os
import re
import shutil
from bsddb3 import db

import nltk


class ConditionalFreqDistDB(object):
    def __init__(self, fname, bAppend=True, samples=None, remold=False):
        """
        mimics nltk.ConditionalFreq but using bsddb3
        conditions are stores in self.conditionDB = FreqDistDB
        Args:
        fname : filename to store the key/value pairs
        bAppend : Open DB for write/append?
        samples : initialize
        remold : remove old 'fname' first

        """
        self._bAppend = bAppend  # Open for write/append?
        self.fname = fname
        self.dbTable = db.DB()
        self.dbTable.set_cachesize(0, 40000000)  # (GB, B)

        def backup(fname):
            if not os.path.exists(fname):
                return
            idx = 0
            fnew = "%s.bak%i" % (fname, idx)
            while os.path.exists(fnew):
                idx += 1
                fnew = "%s.bak%i" % (fname, idx)
            print "Moving %s to %s" % (fname, fnew)
            shutil.move(fname, fnew)

        if remold:
            backup(fname)
            backup('%s.cDB' % fname)

        if (bAppend):
            self.dbTable.open(fname, None,
                              db.DB_HASH, db.DB_DIRTY_READ | db.DB_CREATE )
            self.conDB = FreqDistDB("%s.cDB" % fname, True)
        else:
            self.dbTable.open(fname, None,
                              db.DB_HASH, db.DB_DIRTY_READ )
            self.conDB = FreqDistDB("%s.cDB" % fname, False)
        self._fdists = {}
        if samples:
            self.update(samples)

    def _setdb(self, w1w2, val):
        if (self._bAppend and self.dbTable is not None):
            pk = pickle.dumps(val, pickle.HIGHEST_PROTOCOL)
            self.dbTable.put(w1w2, pk )

    # Increment the count for a particular word
    def _increment(self, w1w2, inc=1):
        if (self._bAppend and self.dbTable is not None):
            v = self._get_w1w2(w1w2)
            pk = pickle.dumps(v + inc, pickle.HIGHEST_PROTOCOL)
            self.dbTable.put(w1w2, pk )

    def update(self, samples):
        """ incremenet the key "(w1, w2)"/value pair """
        try:
            sample_iter = samples.iteritems()
        except:
            sample_iter = map(lambda x: (x, 1), samples)
        for ((cond, sample), count) in sample_iter:
            self.conDB.inc(cond, count=count)
            key = '%s_%s' % (cond, sample)
            self._increment(key, inc=count)

    def __getitem__(self, condition):
        """ returns a nltk.FreqDist """
        return self._get(condition)

    def conditions(self):
        return self.conDB.keys()

    def N(self):
        return sum([self[fdist].N() for fdist in self.conditions()])

    def _get_w1w2(self, w1w2, ret=0):
        if (self.dbTable is not None):
            pk = self.dbTable.get(w1w2)
            if (pk is not None):
                return pickle.loads(pk)
        return ret

    def _get(self, condition, ret=0):
        v = self.conDB.get(condition, None)
        FD = nltk.FreqDist()
        if (v is not None) and (self.dbTable is not None):
            # then we should update all the matched keys
            cursor = self._cursor()
            rec = cursor.first()
            # note: re.escape removes special-character meaning (punctuation)
            m = re.compile(re.escape('%s_' % condition))
            while rec:
                g = m.match('%s_' % rec[0])
                if g is not None:
                    w2 = rec[0][g.end():]
                    FD.inc(w2, self._cursor_value(rec))
                rec = cursor.next()
        return FD

    # Fetch a cursor (standard bsddb3 cursor)
    # Cursor should be read with cursor_key, cursor_value
    def _cursor(self):
        if (self.dbTable is not None):
            return self.dbTable.cursor()
        return None

    # Fetch the key (word) for the current cursor tuple ( cursor.next() )
    def _cursor_key(self, cursor_tuple):
        if (cursor_tuple is not None):
            return cursor_tuple[0]
        return None

    # Fetch the key (value) for the current cursor tuple
    def _cursor_value(self, cursor_tuple):
        if (cursor_tuple is not None):
            return pickle.loads(cursor_tuple[1])
        return None

    # Close this database
    def close(self):
        if (self.dbTable is not None):
            self.dbTable.close()
            self.dbTable = None
            self.conDB.close()
            self.conDB = None

    # Flush all changes or cache to disk
    def flush(self):
        if (self._bAppend):
            self.dbTable.sync()
            self.conDB.flush()


class FreqDistDB(object):
    """
    provide nltk.FreqDist functionality, but with a db on disk (not RAM)

    Note: we move fname to fname.bak
    """
    # Open file for read or write/append
    # Claler should delete file, if new db is required
    def __init__(self, fname, bAppend=True, samples=None, remold=False):
        self._bAppend = bAppend  # Open for write/append?
        self.dbTable = db.DB()
        self.dbTable.set_cachesize(0, 20000000)  # (GB, B)
        self.fname = fname

        def backup(fname):
            if not os.path.exists(fname):
                return
            idx = 0
            fnew = "%s.bak%i" % (fname, idx)
            while os.path.exists(fnew):
                idx += 1
                fnew = "%s.bak%i" % (fname, idx)
            print "Moving %s to %s" % (fname, fnew)
            shutil.move(fname, fnew)

        if remold:
            backup(fname)

        if (bAppend):
            self.dbTable.open(fname, None,
                              db.DB_HASH, db.DB_DIRTY_READ | db.DB_CREATE )
        else:
            self.dbTable.open(fname, None,
                              db.DB_HASH, db.DB_DIRTY_READ )
        self._N = 0
        if samples:
            self.update(samples)

    def update(self, samples):
        """
        Update the frequency distribution with the provided list of samples.
        This is a faster way to add multiple samples to the distribution.

        @param samples: The samples to add.
        @type samples: C{list}
        """
        try:
            sample_iter = samples.iteritems()
        except:
            sample_iter = map(lambda x: (x, 1), samples)
        for sample, count in sample_iter:
            self.inc(sample, count=count)

    def inc(self, sample, count=1):
        """
        Increment this C{FreqDist}'s count for the given
        sample.

        @param sample: The sample whose count should be incremented.
        @type sample: any
        @param count: The amount to increment the sample's count by.
        @type count: C{int}
        @rtype: None
        @raise NotImplementedError: If C{sample} is not a
               supported sample type.
        """
        if count == 0: return
        self[sample] = self.get(sample, 0) + count

    # give dictionary-like access to words
    def __getitem__(self, word):
        return self.get(word)

    def __setitem__(self, word, value):
        self.setdb(word, val=value)

    # mimic nltk.FreqDist functionality
    def N(self):
        return self.calculate_total()

    def B(self):
        nTotal = 0L
        if (self.dbTable is not None):
            cursor = self.cursor()
            rec = cursor.first()
            while rec:
                if (rec[0] != '__total__'):
                    if self.cursor_value(rec) > 0:
                        nTotal += 1
                rec = cursor.next()
        return nTotal

    def freq(self, sample):
        if self.N() is 0:
            return 0
        else:
            return float(self[sample]) / self.N()

    # Close this database
    def close(self):
        if (self.dbTable is not None):
            self.dbTable.close()
            self.dbTable = None

    # Flush all changes or cache to disk
    def flush(self):
        if (self._bAppend):
            self.dbTable.sync()

    def setdb(self, word, val):
        if (self._bAppend and self.dbTable is not None):
            v = self.get(word)
            pk = pickle.dumps(val, pickle.HIGHEST_PROTOCOL)
            self.dbTable.put(word, pk )
            self._N += val - v

    # Increment the count for a particular word
    def increment(self, word, inc=1):
        if (self._bAppend and self.dbTable is not None):
            v = self.get(word)
            pk = pickle.dumps(v + inc, pickle.HIGHEST_PROTOCOL)
            self.dbTable.put(word, pk )
            self._N += inc

    # Query the count for a particular word
    # "not found" implies a value of zero
    def get(self, word, ret=0):
        if (self.dbTable is not None):
            pk = self.dbTable.get(word)
            if (pk is not None):
                return pickle.loads(pk)
        return ret

    def keys(self):
        """
        Return the samples sorted in decreasing order of frequency.

        @return: A list of samples, in sorted order
        @rtype: C{list} of any
        """
        keys = []
        if (self.dbTable is not None):
            cursor = self.cursor()
            rec = cursor.first()
            while rec:
                if (rec[0] != '__total__'):
                    keys.append(rec[0])
                rec = cursor.next()
        return keys

    # Fetch a cursor (standard bsddb3 cursor)
    # Cursor should be read with cursor_key, cursor_value
    def cursor(self):
        if (self.dbTable is not None):
            return self.dbTable.cursor()
        return None

    # Fetch the key (word) for the current cursor tuple ( cursor.next() )
    def cursor_key(self, cursor_tuple):
        if (cursor_tuple is not None):
            return cursor_tuple[0]
        return None

    # Fetch the key (value) for the current cursor tuple
    def cursor_value(self, cursor_tuple):
        if (cursor_tuple is not None):
            return pickle.loads(cursor_tuple[1])
        return None

    # Calculate the total count for the entire table
    # Count is returned, and also saved as "__total__"
    def calculate_total(self):
        nTotal = 0L
        if (self.dbTable is not None):
            cursor = self.cursor()
            rec = cursor.first()
            while rec:
                if (rec[0] != '__total__'):
                    nTotal = nTotal + self.cursor_value(rec)
                rec = cursor.next()
            pk = pickle.dumps(nTotal, pickle.HIGHEST_PROTOCOL)
            self.dbTable.put('__total__', pk )
        return nTotal


class ConditionalProbDistDB(nltk.ConditionalProbDistI):
    """
    modelled after nltk.ConditionalProbDist, but we
    only extract the dists if we need them.
    (nltk pre-extracts )

    """
    def __init__(self, cfdist, Ncfd, probdist_factory, useDB=False,
                 skipsmall=0,
                 *factory_args, **factory_kw_args):
        """
        Construct a new conditional probability distribution, based on
        the given conditional frequency distribution and C{ProbDist}
        factory.

        @type cfdist: L{ConditionalFreqDist}
        @param cfdist: The C{ConditionalFreqDist} specifying the
            frequency distribution for each condition.
        Ncfd : a number to help distinguish database names
        @type probdist_factory: C{class} or C{function}
        @param probdist_factory: The function or class that maps
            a condition's frequency distribution to its probability
            distribution.  The function is called with the frequency
            distribution as its first argument,
            C{factory_args} as its remaining arguments, and
            C{factory_kw_args} as keyword arguments.
        useDB: [False] use FreqDistDB instead of nltk.FreqDist
        skipsmall : [0] skip conditions with fewer than this many samples
        @type factory_args: (any)
        @param factory_args: Extra arguments for C{probdist_factory}.
            These arguments are usually used to specify extra
            properties for the probability distributions of individual
            conditions, such as the number of bins they contain.
        @type factory_kw_args: (any)
        @param factory_kw_args: Extra keyword arguments for C{probdist_factory}
        """
        self._probdist_factory = probdist_factory
        self._cfdist = cfdist

        self._cfdists = {}
        self._factory_args = factory_args
        self._factory_kw_args = factory_kw_args
        self._pdists = cfdist.conditions()
        # keep pdists in memory if we've seen the keyword a few times
        self._pdistsseen = nltk.FreqDist()
        self._pdistskeep = {}
        # distributions to skip
        self._skipsmall = skipsmall
        self._pdistskip = {}

        self.Ncfd = Ncfd
        self.useDB = useDB

    def __contains__(self, condition):
        return condition in self._pdists

    def __getitem__(self, condition):
        # track the number of times we've seen each condition
        self._pdistsseen[condition] += 1
        if condition in self._pdistskip:
            ret = self._pdistskip[condition]
        elif condition in self._pdistskeep:
            ret = self._pdistskeep[condition]
        elif condition not in self._pdists:
            # If it's a condition we haven't seen, create a new prob
            # dist from the empty freq dist.  Typically, this will
            # give a uniform prob dist.
            pdist = self._probdist_factory(nltk.FreqDist(),
                                           *self._factory_args,
                                           **self._factory_kw_args)
            ret = pdist
        #            self._pdists.append(condition)
            self._cfdists[condition] = nltk.FreqDist()
        else:
            fd = self._cfdist[condition]
            if self.useDB:
                dbdir = '' #TODO
                fname = dbdir + '%03i_%s.pdb' % (self.Ncfd, condition)
                fd = nltk.FreqDistDB(fname, samples=fd)
                fd.flush()
            ret = self._probdist_factory(fd, *self._factory_args,
                                         **self._factory_kw_args)
        # keep pdf's of popular items in memory
        if self._pdistsseen[condition] >= 2:
            self._pdistskeep[condition] = ret
        # keep track of pdf's with very few samples
        if len(ret.samples()) <= self._skipsmall:
            self._pdistskip[condition] = self._probdist_factory(nltk.FreqDist(),
                                                                *self._factory_args,
                                                                **self._factory_kw_args)
        return ret

    def conditions(self):
        return self._pdists

    def __len__(self):
        return len(self._pdists)
