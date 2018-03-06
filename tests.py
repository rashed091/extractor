from __future__ import unicode_literals, division

import spacy
import sys
import argparse
import bz2
import fileinput
import logging
import os.path
import re
import time
import ujson
from io import StringIO
from multiprocessing import Queue, Process, Value, cpu_count
from timeit import default_timer


# ===========================================================================
pre_format_re = re.compile(r'^[\`\*\~]')
post_format_re = re.compile(r'[\`\*\~]$')
url_re = re.compile(r'\[([^]]+)\]\(%%URL\)')
link_re = re.compile(r'\[([^]]+)\]\(https?://[^\)]+\)')

# ===========================================================================
LABELS = {
    'ENT': 'ENT',
    'PERSON': 'ENT',
    'NORP': 'ENT',
    'FAC': 'ENT',
    'ORG': 'ENT',
    'GPE': 'ENT',
    'LOC': 'ENT',
    'LAW': 'ENT',
    'PRODUCT': 'ENT',
    'EVENT': 'ENT',
    'WORK_OF_ART': 'ENT',
    'LANGUAGE': 'ENT',
    'DATE': 'DATE',
    'TIME': 'TIME',
    'PERCENT': 'PERCENT',
    'MONEY': 'MONEY',
    'QUANTITY': 'QUANTITY',
    'ORDINAL': 'ORDINAL',
    'CARDINAL': 'CARDINAL'
}


# ===========================================================================
def createLogger(quiet, debug):
    logger = logging.getLogger()
    if not quiet:
        logger.setLevel(logging.INFO)
    if debug:
        logger.setLevel(logging.DEBUG)

# ======================================================================

class Extractor(object):
    """
    An extraction task on a article.
    """
    def __init__(self, id, text):
        self.id = id
        self.text = text
        self.nlp = spacy.load('en_core_web_sm')

    def write_output(self, out, text):
        """
        :param out: a memory file
        :param text: the text of the page
        """
        if out == sys.stdout:  # option -a or -o -
            text = text.encode('utf-8')
        out.write(str(text))
        out.write('\n')

    def extract(self, out):
        """
        :param out: a memory file.
        """
        text = self.text
        self.text = ''  # save memory

        text = self.strip_meta(text)
        # text = self.clean(text)

        doc = self.nlp(text)
        text = self.transform_doc(doc)
        self.write_output(out, text)

    def strip_meta(self, text):
        # residuals of unbalanced quotes
        text = link_re.sub(r'\1', text)
        text = text.replace('&gt;', '>').replace('&lt;', '<')
        text = pre_format_re.sub('', text)
        text = post_format_re.sub('', text)
        text = text.replace('\\', '')
        return text


    def transform_doc(self, doc):
        for ent in doc.ents:
            ent.merge(tag=ent.root.tag_, lemma=ent.text, ent_type=LABELS[ent.label_])
        for np in doc.noun_chunks:
            while len(np) > 1 and np[0].dep_ not in ('advmod', 'amod', 'compound'):
                np = np[1:]
            np.merge(tag=np.root.tag_, lemma=np.text, ent_type=np.root.ent_type_)
        strings = []
        for sent in doc.sents:
            if sent.text.strip():
                strings.append(' '.join(self.represent_word(w) for w in sent if not w.is_space))
        if strings:
            return '\n'.join(strings) + '\n'
        else:
            return ''

    def represent_word(self, word):
        if word.like_url:
            return '%%URL|X'
        text = re.sub(r'\s', '_', word.text)
        tag = LABELS.get(word.ent_type_, word.pos_)
        if not tag:
            tag = '?'
        return text + '|' + tag

# ----------------------------------------------------------------------
# Output


class NextFile(object):
    """
    Synchronous generation of next available file name.
    """

    filesPerDir = 100

    def __init__(self, path_name):
        self.path_name = path_name
        self.dir_index = -1
        self.file_index = -1

    def __next__(self):
        self.file_index = (self.file_index + 1) % NextFile.filesPerDir
        if self.file_index == 0:
            self.dir_index += 1
        dirname = self._dirname()
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
        return self._filepath()

    next = __next__

    def _dirname(self):
        char1 = self.dir_index % 26
        char2 = self.dir_index // 26 % 26
        return os.path.join(self.path_name, '%c%c' % (ord('A') + char2, ord('A') + char1))

    def _filepath(self):
        return '%s/reddit_%02d' % (self._dirname(), self.file_index)


class OutputSplitter(object):
    """
    File-like object, that splits output to multiple files of a given max size.
    """

    def __init__(self, nextFile, max_file_size=0, compress=True):
        """
        :param nextFile: a NextFile object from which to obtain filenames
            to use.
        :param max_file_size: the maximum size of each file.
        :para compress: whether to write data with bzip compression.
        """
        self.nextFile = nextFile
        self.compress = compress
        self.max_file_size = max_file_size
        self.file = self.open(next(self.nextFile))

    def reserve(self, size):
        if self.file.tell() + size > self.max_file_size:
            self.close()
            self.file = self.open(next(self.nextFile))

    def write(self, data):
        self.reserve(len(data))
        self.file.write(data)

    def close(self):
        self.file.close()

    def open(self, filename):
        if self.compress:
            return bz2.BZ2File(filename + '.bz2', 'w')
        else:
            return open(filename, 'wb')


# ----------------------------------------------------------------------

def process_dump(input_file, out_file, file_size, file_compress, process_count):
    """
    :param input_file: name of the wikipedia dump file; '-' to read from stdin
    :param template_file: optional file with template definitions.
    :param out_file: directory where to store extracted data, or '-' for stdout
    :param file_size: max size of each extracted file, or None for no max (one file)
    :param file_compress: whether to compress files with bzip.
    :param process_count: number of extraction processes to spawn.
    """
    if input_file == '-':
        input = sys.stdin
    else:
        input = fileinput.FileInput(input_file, openhook=fileinput.hook_compressed)

    logging.info("Starting page extraction from %s.", input_file)
    extract_start = default_timer()

    # Parallel Map/Reduce:
    # - pages to be processed are dispatched to workers
    # - a reduce process collects the results, sort them and print them.
    options = {}
    process_count = max(1, process_count)
    maxsize = 10 * process_count
    # output queue
    output_queue = Queue(maxsize=maxsize)

    if out_file == '-':
        out_file = None

    worker_count = process_count

    # load balancing
    max_spool_length = 10000
    spool_length = Value('i', 0, lock=False)

    # reduce job that sorts and prints output
    reduce = Process(target=reduce_process,
                     args=(options, output_queue, spool_length, out_file, file_size, file_compress))
    reduce.start()

    # initialize jobs queue
    jobs_queue = Queue(maxsize=maxsize)

    # start worker processes
    logging.info("Using %d extract processes.", worker_count)
    workers = []
    for i in range(worker_count):
        extractor = Process(target=extract_process,
                            args=(options, i, jobs_queue, output_queue))
        extractor.daemon = True  # only live while parent process lives
        extractor.start()
        workers.append(extractor)

    # Mapper process
    page_num = 0
    id  = 0
    for comment in text_from(input):
        # slow down
        delay = 0
        if spool_length.value > max_spool_length:
            # reduce to 10%
            while spool_length.value > max_spool_length / 10:
                time.sleep(10)
                delay += 10
        if delay:
            logging.info('Delay %ds', delay)
        job = (id, comment, page_num)
        jobs_queue.put(job)  # goes to any available extract_process
        page_num += 1
        id += 1

    input.close()

    # signal termination
    for _ in workers:
        jobs_queue.put(None)
    # wait for workers to terminate
    for w in workers:
        w.join()

    # signal end of work to reduce process
    output_queue.put(None)
    # wait for it to finish
    reduce.join()

    extract_duration = default_timer() - extract_start
    extract_rate = page_num / extract_duration
    logging.info("Finished %d-process extraction of %d articles in %.1fs (%.1f art/s)",
                 process_count, page_num, extract_duration, extract_rate)
    print('Finished {}-process extraction of {} articles in {} ({} com/s)'.format(process_count, page_num, extract_duration, extract_rate))

# ----------------------------------------------------------------------
# Multiprocess support


def extract_process(opts, i, jobs_queue, output_queue):
    """Pull tuples of raw page content, do CPU/regex-heavy fixup, push finished text
    :param i: process id.
    :param jobs_queue: where to get jobs.
    :param output_queue: where to queue extracted text for output.
    """

    createLogger(True, False)

    out = StringIO()  # memory buffer

    while True:
        job = jobs_queue.get()  # job is (id, comment, page_num)
        if job:
            id, comment, page_num = job
            try:
                e = Extractor(*job[:2])  # (id, comment)
                page = None  # free memory
                e.extract(out)
                text = out.getvalue()
            except:
                text = ''
                logging.exception('Processing page: %s %s', id, comment)

            output_queue.put((page_num, text))
            out.truncate(0)
            out.seek(0)
        else:
            logging.debug('Quit extractor')
            break
    out.close()


def reduce_process(opts, output_queue, spool_length, out_file=None, file_size=0, file_compress=True):
    """Pull finished article text, write series of files (or stdout)
    :param opts: global parameters.
    :param output_queue: text to be output.
    :param spool_length: spool length.
    :param out_file: filename where to print.
    :param file_size: max file size.
    :param file_compress: whether to compress output.
    """

    createLogger(True, False)

    if out_file:
        nextFile = NextFile(out_file)
        output = OutputSplitter(nextFile, file_size, file_compress)
    else:
        logging.warning("writing to stdout, so no output compression (use an external tool)")

    interval_start = default_timer()
    # FIXME: use a heap
    spool = {}  # collected pages
    next_page = 0  # sequence numbering of page
    while True:
        if next_page in spool:
            output.write(spool.pop(next_page).encode('utf-8'))
            next_page += 1
            # tell mapper our load:
            spool_length.value = len(spool)
            # progress report
            if next_page % report_period == 0:
                interval_rate = report_period / (default_timer() - interval_start)
                logging.info("Extracted %d articles (%.1f art/s)",
                             next_page, interval_rate)
                interval_start = default_timer()
        else:
            # mapper puts None to signal finish
            pair = output_queue.get()
            if not pair:
                break
            page_num, text = pair
            spool[page_num] = text
            # tell mapper our load:
            spool_length.value = len(spool)
            # FIXME: if an extractor dies, process stalls; the other processes
            # continue to produce pairs, filling up memory.
            if len(spool) > 200:
                logging.debug('Collected %d, waiting: %d, %d', len(spool),
                              next_page, next_page == page_num)
    if output != sys.stdout:
        output.close()


# ----------------------------------------------------------------------
def text_from(file_):
    comments = []
    for i, line in enumerate(file_):
        comments.append(ujson.loads(line)['body'])
    return comments

# ----------------------------------------------------------------------

report_period = 100  # progress report period

def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("input",
                        help="XML wiki dump file")
    groupO = parser.add_argument_group('Output')
    groupO.add_argument("-o", "--output", default="text",
                        help="directory for extracted files (or '-' for dumping to stdout)")
    groupO.add_argument("-c", "--compress", action="store_true",
                        help="compress output files using bzip")
    groupO.add_argument("--json", action="store_true",
                        help="write output in json format instead of the default one")


    default_process_count = max(1, cpu_count() - 4)
    print(default_process_count)
    parser.add_argument("--processes", type=int, default=default_process_count,
                        help="Number of processes to use (default %(default)s)")

    args = parser.parse_args()

    FORMAT = '%(levelname)s: %(message)s'
    logging.basicConfig(format=FORMAT)

    createLogger(True, False)

    input_file = args.input

    output_path = args.output
    if output_path != '-' and not os.path.isdir(output_path):
        try:
            os.makedirs(output_path)
        except:
            logging.error('Could not create: %s', output_path)
            return
    # Minimum size of output files
    file_size = 3000 * 1024

    process_dump(input_file, output_path, file_size, args.compress, args.processes)


if __name__ == '__main__':
    main()
