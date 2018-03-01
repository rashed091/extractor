#!/usr/bin/env python

import multiprocessing
import os
import requests

########################################################################
class MultiProcDownloader(object):
    """
    Downloads urls with Python's multiprocessing module
    """

    #----------------------------------------------------------------------
    def __init__(self, urls):
        """ Initialize class with list of urls """
        self.urls = urls

    #----------------------------------------------------------------------
    def run(self):
        """
        Download the urls and waits for the processes to finish
        """
        jobs = []
        for url in self.urls:
            process = multiprocessing.Process(target=self.worker, args=(url,))
            jobs.append(process)
            process.start()
        for job in jobs:
            job.join()

    #----------------------------------------------------------------------
    def worker(self, url):
        """
        The target method that the process uses tp download the specified url
        """
        fname = os.path.basename(url)
        msg = "Starting download of %s" % fname
        print(msg, multiprocessing.current_process().name)
        r = requests.get(url)
        with open(fname, "wb") as f:
            f.write(r.content)

#----------------------------------------------------------------------
if __name__ == "__main__":
    urls = ["http://www.irs.gov/pub/irs-pdf/f1040.pdf",
            "http://www.irs.gov/pub/irs-pdf/f1040a.pdf",
            "http://www.irs.gov/pub/irs-pdf/f1040ez.pdf",
            "http://www.irs.gov/pub/irs-pdf/f1040es.pdf",
            "http://www.irs.gov/pub/irs-pdf/f1040sb.pdf"]
    downloader = MultiProcDownloader(urls)
    downloader.run()