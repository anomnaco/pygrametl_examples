# A program for generating example data for "pygrametl: A Powerful Programming 
# Framework for Extract--Transform--Load Programmers"

#  
#  Copyright (c) 2009 Christian Thomsen (chr@cs.aau.dk)
#  
#  This file is free software: you may copy, redistribute and/or modify it  
#  under the terms of the GNU General Public License version 2 
#  as published by the Free Software Foundation.
#  
#  This file is distributed in the hope that it will be useful, but  
#  WITHOUT ANY WARRANTY; without even the implied warranty of  
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU  
#  General Public License for more details.  
#  
#  You should have received a copy of the GNU General Public License  
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.  
#  

# Note that you can control the size of the generated data by setting
# values for
#  - toplevels  (No. of toplevels                     - default 5)
#  - domains    (No. of domains for each toplevel     - default 5)
#  - pages      (No. of pages in each domain          - default 10)
#  - months     (No. of months with test results      - default 6)
#  - changeprob (Change probability (in %) for a page - default 50)
#  - tests      (No. of tests generating results      - default 5)
# in a file called params.py

import random

try:
    from params import toplevels
except ImportError:
    toplevels = 5

try:
    from params import domains
except ImportError:
    domains = 5

try:
    from params import pages
except ImportError:
    pages = 10

try:
    from params import months
except ImportError:
    months = 6

try:
    from params import changeprob
except ImportError:
    changeprob = 50

try:
    from params import startyear
except ImportError:
    startyear = 2008

try:
    from params import tests
except ImportError:
    tests = 5

def generateurls():
    toplevellist = ["tl%d" % (i,) for i in range(toplevels)]
    domainlist = ["domain%d" % (i,) for i in range(domains)] # in each toplevel
    pagelist = ["page%d.html" % (i,) for i in range(pages)]# in each domain

    for t in toplevellist:
        for d in domainlist:
            for p in pagelist:
                yield "http://www.%s.%s/%s" % (d, t, p)

def generatedownloadlog(outfile):
    servers = ['SomeServer/1.0', 'SomeServer/2.0', 'SuperServer/3.0']
    cache = {}
    rnd = random.Random()
    rnd.seed(1) # We want to be able to recreate results
    
    i = 0
    year = startyear
    for month in range(1, months + 1):
        urls = generateurls()
        if month > 1 and month % 12 == 1: # It is January in a new year
            year += 1  
        downloaddate = "%d-%02d-01" % (year,
                                       month % 12 == 0 and 12 or month % 12)
        for url in urls:
            i += 1
            localfile = "%08d.tmp" % (i,)
            # With probability 100 - changeprob, the page wasn't changed
            tmp = rnd.randint(1,100)
            if tmp <= 100 - changeprob and url in cache:
                line = cache.get(url)
                line[0] = localfile
                line[4] = downloaddate
                writeline(outfile, line)
                continue
            # Else simulate some changes...

            # A given domain is always using the same server in this example.
            # We extract the number in the domain and use that to determine the
            # server.
            tmp = int(url.split(".tl")[0].split("domain")[1])
            server = servers[tmp % len(servers)]
            size = rnd.randint(100, 15000)

            tmp = rnd.randint(2,28)
            if month % 12 == 1:
                lastmoddate = "%d-12-%02d" % (year - 1, tmp)
            elif month % 12 == 0:
                lastmoddate = "%d-11-%02d" % (year, tmp)
            else:
                lastmoddate = "%d-%02d-%02d" % (year, month % 12 - 1, tmp)
            line = [localfile, url, server, size, downloaddate, lastmoddate]
            cache[url] = line
            writeline(outfile, line)



def generatetestresults(logfile, outfile):
    testlist = ['Test%d' % (i,) for i in range(tests)]
    logfile.readline() # Skip headers
    for logline in logfile:
        logfields = logline.split('\t')
        localfile = logfields[0]
        for test in testlist:
            # We "calculate" the errors (instead of taking a random number)
            # such that we get the same results when we consider an
            # unchanged file
            no1 = int(logfields[3]) # size
            no2 = int(test.split('t')[-1])  # The test number
            no3 = int(logfields[5].split('-')[-1]) # day af lastmoddate
            errors = (no2 * no1) % no3
            line = (localfile, test, errors)
            writeline(outfile, line)


def writeline(outfile, fields):
    outfile.write("\t".join([str(f) for f in fields]))
    outfile.write("\n")



import sys
if __name__ == "__main__":
    downloadlog = open('/workspace/pygrametl_examples/data/DownloadLog.csv', 'w+', 16384)
    testresults = open('/workspace/pygrametl_examples/data/TestResults.csv', 'w', 16384)
    writeline(downloadlog, ['localfile', 'url', 'serverversion', 'size',
                            'downloaddate', 'lastmoddate'])
    writeline(testresults, ['localfile', 'test', 'errors'])
    generatedownloadlog(downloadlog)
    downloadlog.seek(0)
    generatetestresults(downloadlog, testresults)
    downloadlog.close()
    testresults.close()
