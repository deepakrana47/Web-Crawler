sites=[
    'http://www.yogiapproved.com',
    ]

from HTMLParser import HTMLParser
from htmlentitydefs import name2codepoint
import re, time, pickle,os
global stime
stime = time.time()

def get_time():
     global stime
     a = time.time() - stime
     stime = time.time()
     return a

class MyHTMLParser(HTMLParser, object):

    def __init__(self):
        # self.curr = ''
        self.all_data = []
        self.pdata = ''
        self.ptag = ''
        super(MyHTMLParser, self).__init__()

    def handle_data(self, data):
        data = re.sub(r'[\t\n\r]','', data).strip(' ')
        if data and self.lasttag not in ['script', 'style']:
            self.all_data.append((self.iter, self.lasttag, data))

from crawler_v2 import Crawler, get_hash
from dbase import DB
impTags = ['h1','h2','h3','h4','h5','h6','p','span']
def pipeline(dtext):
    # geting domain_name
    domains = []
    if type(dtext) == list:
        domains = dtext
    elif type(dtext) == str:
        domains = open(dtext, 'r').read().strip('\n\t ').split('\n')
    else:
        print "Input should be list or file containing domains (http://www.xyz.com)"
        exit()
    dedata_db = DB('dedata.db')
    dedata_db.query('create table if not exists domain_data (dname text , url text, hash int, content text)')
    # dedata = open('dedata.txt','a')
    crawler = Crawler()
    count = 1
    if os.path.isfile('crawler_data.pkl'):
        crawler.domain_hash, crawler.content, crawler.n_urls, crawler.conti, crawler.refresh, crawler.domain, crawler.scheme, crawler.u_parse, count = pickle.load(open('crawler_data.pkl','rb'))
    try:
        while domains or crawler.conti:
            ## crawling part
            get_time()
            domains, _ = crawler.crawl(domains, count)
            print "crawling time:",get_time()
            ## parsing or data extraction part
            parser = MyHTMLParser()
            data=''
            for key in crawler.content[crawler.domain]:
                count+=1
                parser.feed(crawler.content[crawler.domain][key])
                print '\n%s:'%(key)
                print "parsing time:",get_time()
                for i in parser.all_data:
                    if i[1] in impTags and i[2]:
                        data += '%s\t%s\t%s\t\t'%(str(i[0]),i[1],i[2])
                dedata_db.insert("INSERT INTO domain_data (dname, url, hash, content) VALUES ('%s', '%s', %d, '%s')"
                                 %(crawler.domain, key, get_hash('%s://%s%s' % (crawler.scheme, crawler.domain, key)), data.replace("'","''")))
                print "dbase entry time:",get_time()
            crawler.content[crawler.domain] = {}
            print "\n%d pages processed"%(count)
    except KeyboardInterrupt as e:
        pickle.dump([crawler.domain_hash, crawler.content , crawler.n_urls, crawler.conti, crawler.refresh, crawler.domain, crawler.scheme, crawler.u_parse, count], open('crawler_data.pkl','wb'))
        pass

if __name__=="__main__":
    domains = ['https://www.yogiapproved.com/']
    t = time.time()
    # domains = ['http://www.gbpuat.ac.in/']
    pipeline(domains)
    print "Total time taken:",time.time()-t
