# -*- coding: utf-8 -*-
# filename: crawler.py

import sqlite3, re
import urllib2, requests
from HTMLParser import HTMLParser
from urlparse import urlparse
from dbase import DB
import os,socket
import threading
import Queue

re_hrefs = re.compile(r'^[a-zA-Z0-9_\-]+/[^\.]+\.(html|htm|php)')

class HREFParser(HTMLParser):
    """
    Parser that extracts hrefs
    """
    hrefs = set()
    def handle_starttag(self, tag, attrs):
        # print "Start tag:", tag, self.iter
        if tag == 'p':
            self.ptag = 'p'
        # for attr in attrs:
        #     # print "     attr:", attr
        if tag == 'a':
            dict_attrs = dict(attrs)
            if dict_attrs.get('href'):
                self.hrefs.add(dict_attrs['href'])

def isDomainexist(domain_name):
    try:
        socket.gethostbyname(domain_name.strip())
    except socket.gaierror:
        print "unable to get address for", domain_name
        return -1
    return 1

def get_links(html, domain):
    """
    Read through HTML content and returns a tuple of links
    internal to the given domain
    """
    local_hrefs = set()
    external_hrefs = set()
    parser = HREFParser()
    parser.feed(html)
    for href in parser.hrefs:
        u_parse = urlparse(href)
        if href.startswith('/'):
            # purposefully using path, no query, no hash
            local_hrefs.add(u_parse.path)
        elif re_hrefs.search(href):
            # purposefully using path, no query, no hash
            local_hrefs.add('/'+u_parse.path)
        else:
          # only keep the local urls
          if u_parse.netloc == domain:
            local_hrefs.add(u_parse.path)
          elif u_parse.netloc and u_parse.netloc != domain:
            external_hrefs.add(href)
    return list(local_hrefs), list(external_hrefs)

def get_hash(link):
    return hash(link)

class DBHandle(DB):
    def __init__(self, domain_name_db, domain_data_db):
        self.dname_db = DB(domain_name_db)
        self.ddata_db = DB(domain_data_db)
        self.dname_db.query('create table if not exists domain_name (dname text, hash int primary key)')
        self.ddata_db.query('create table if not exists domain_data (dname text , url text, hash int primary key, content text)')
        # self.durl_db = DB(domain_url)

    def query_dname(self, query):
        self.dname_db.query(query)

    def query_ddata(self, query):
        self.ddata_db.query(query)

    def insert_dname(self, query):
        self.dname_db.query(query)

    def insert_ddata(self, query):
        self.ddata_db.query(query)


class Crawler(object):
    def __init__(self, cache=None, depth=2, domain_name='dname.db', domain_url = 'durl.db', domain_data='ddata.db'):
        """
        depth: how many time it will bounce from page one (optional)
        cache: a basic cache controller (optional)
        """
        self.dbHandle = DBHandle(domain_name_db=domain_name, domain_data_db=domain_data)
        self.domain_hash = []
        self.content = {}
        self.cache = cache
        hdata = self.dbHandle.query_ddata("select hash from domain_data;")
        self.hashes = set( [i[0] for i in hdata]) if hdata else set()
        self.n_urls = None
        self.conti = 0
        self.refresh = 0
        self.q = Queue.Queue()
        self.lock = threading.RLock()

    def isDomaincrawled(self, hash):
        out = self.dbHandle.query_dname('select dname from domain_name where hash = %d;'%(hash))
        return 1 if out else 0

    def crawl(self, domains, count=0, no_cache=None):
        """
        domains: domain names where crawling is started, should be a complete URL like
        no_cache: function returning True if the url should be refreshed
        """
        external_links =[]
        # Iterating over domain names
        while domains:
            domain = domains[0]
            self.hashes.add(get_hash(''))
            self.u_parse = urlparse(domain)
            self.domain, self.scheme, self.no_cache = self.u_parse.netloc, self.u_parse.scheme, no_cache
            if not isDomainexist(self.domain): continue
            if self.isDomaincrawled(get_hash(self.domain)): continue
            if not self.conti:
                self.content[self.domain] = {}
            external_links += self._crawl([self.u_parse.path], count)
            if not self.conti:
                try: self.dbHandle.insert_dname("INSERT INTO domain_name (dname, hash) VALUES ('%s', %d);"%(self.domain, get_hash(self.domain)))
                except sqlite3.IntegrityError as e: print e.message
                domains.pop(0)
            return domains, external_links

    def set(self, url, html):
        self.content[self.domain][url] = html

    def get(self, url, count):
        page = self.curl(url, count)
        return page

    def is_cacheable(self, url):
        return self.cache and self.no_cache \
            and not self.no_cache(url)

    def getReq(self, url, count):
        html = self.get(url, count)
        self.lock.acquire()
        llinks, elinks = get_links(html, self.domain)
        self.set(url, html)
        self.lock.release()
        self.q.put({url: {'local': llinks, 'external': elinks, 'count': count}})

    def _crawl(self, iurl, count):
        external_links = []
        n_urls = self.n_urls if self.n_urls else ['']
        ite = 0
        while n_urls and ite < 8:
            thread = []
            a = n_urls[:4]
            for url in a:
                thread.append((threading.Thread(target=self.getReq, args=(url, count))))
                thread[-1].start()
                count += 1
            for t in thread: t.join()
            r={}
            while not self.q.empty():
                r.update(self.q.get())
            for i in range(4):
                if n_urls:
                    n_urls.pop(0)
                else:
                    break
            for i in r:
                if r[i] is not None:
                    for url in r[i]['local']:
                        if get_hash(url) not in self.hashes:
                            self.hashes.add(get_hash(url))
                            n_urls += [url]
                        else:
                            None
                    external_links = list(set(external_links + r[i]['external']))
            ite+=len(a)
        if len(n_urls) != 0:
            self.conti = 1
            self.n_urls = n_urls
        else:
            self.conti = 0
            self.n_urls = []
        return external_links

    def curl(self, url, count):
        """
        return content at url.
        return empty string if response raise an HTTPError (not found, 500...)
        """
        h = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:58.0) Gecko/20100101 Firefox/58.0'}
        try:
            print "%d retrieving url... [%s] %s" % (count, self.domain, url)
            for i in range(10):
                try:
                    response = requests.get('%s://%s%s' % (self.scheme, self.domain, url), headers=h, timeout=2.0)
                    break
                except requests.Timeout as e:
                    print i,"timeout"
            return re.sub(r'[^\x00-\x7F]',' ', response.text)
        except urllib2.HTTPError, e:
            print "error [%s] %s: %s" % (self.domain, url, e)
            return ''


