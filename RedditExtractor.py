from __future__ import unicode_literals, division

import sys
import argparse
import bz2
import fileinput
import logging
import os.path
import re
import time
import json
from io import StringIO
from multiprocessing import Queue, Process, Value, cpu_count
from timeit import default_timer
from itertools import zip_longest

# ===========================================================================
def createLogger(quiet, debug):
    logger = logging.getLogger()
    if not quiet:
        logger.setLevel(logging.INFO)
    if debug:
        logger.setLevel(logging.DEBUG)


# ===========================================================================
# Match preformatted lines
preformatted = re.compile(r'^ .*?$')

# Matches bold/italic
bold_italic = re.compile(r"'''''(.*?)'''''")
bold = re.compile(r"'''(.*?)'''")
italic_quote = re.compile(r"''\"([^\"]*?)\"''")
italic = re.compile(r"''(.*?)''")
quote_quote = re.compile(r'""([^"]*?)""')

# Matches space
spaces = re.compile(r' {2,}')

# Matches dots
dots = re.compile(r'\.{4,}')


# ======================================================================

class Extractor(object):
    """
    An extraction task on a article.
    """

    def __init__(self, id, revid, title, lines):
        """
        :param id: id of page.
        :param title: tutle of page.
        :param lines: a list of lines.
        """
        self.title = title
        self.text = ''.join(lines)


    def write_output(self, out, text):
        """
        :param out: a memory file
        :param text: the text of the page
        """
        url = get_url(self.id)
        if options.write_json:
            json_data = {
                'id': self.id,
                'url': url,
                'title': self.title,
                'text': "\n".join(text)
            }
            if options.print_revision:
                json_data['revid'] = self.revid
            # We don't use json.dump(data, out) because we want to be
            # able to encode the string if the output is sys.stdout
            out_str = json.dumps(json_data, ensure_ascii=False)
            if out == sys.stdout:  # option -a or -o -
                out_str = out_str.encode('utf-8')
            out.write(out_str)
            out.write('\n')
        else:
            if options.print_revision:
                header = '<doc id="%s" revid="%s" url="%s" title="%s">\n' % (self.id, self.revid, url, self.title)
            else:
                header = '<doc id="%s" url="%s" title="%s">\n' % (self.id, url, self.title)
            footer = "\n</doc>\n"
            if out == sys.stdout:  # option -a or -o -
                header = header.encode('utf-8')
            out.write(header)
            for line in text:
                if out == sys.stdout:  # option -a or -o -
                    line = line.encode('utf-8')
                out.write(line)
                out.write('\n')
            out.write(footer)

    def extract(self, out):
        """
        :param out: a memory file.
        """
        logging.info('%s\t%s', self.id, self.title)

        # Separate header from text with a newline.
        if options.toHTML:
            title_str = '<h1>' + self.title + '</h1>'
        else:
            title_str = self.title + '\n'
        # https://www.mediawiki.org/wiki/Help:Magic_words
        colon = self.title.find(':')
        if colon != -1:
            ns = self.title[:colon]
            pagename = self.title[colon + 1:]
        else:
            ns = ''  # Main
            pagename = self.title
        text = self.text
        self.text = ''  # save memory

        text = self.transform(text)
        text = self.wiki2text(text)
        # text = compact(self.clean(text))
        text = [title_str] + text

        if sum(len(line) for line in text) < options.min_text_length:
            return

        self.write_output(out, text)

        errs = (self.template_title_errs,
                self.recursion_exceeded_1_errs,
                self.recursion_exceeded_2_errs,
                self.recursion_exceeded_3_errs)
        if any(errs):
            logging.warn("Template errors in article '%s' (%s): title(%d) recursion(%d, %d, %d)",
                         self.title, self.id, *errs)


    def wiki2text(self, text):
        # residuals of unbalanced quotes
        text = text.replace("'''", '').replace("''", '"')

        return text

    def clean(self, text):
        """
        Removes irrelevant parts from :param: text.
        """

        # Collect spans
        spans = []
        # Drop HTML comments
        for m in comment.finditer(text):
            spans.append((m.start(), m.end()))

        # Drop self-closing tags
        for pattern in selfClosing_tag_patterns:
            for m in pattern.finditer(text):
                spans.append((m.start(), m.end()))

        # Drop ignored tags
        for left, right in options.ignored_tag_patterns:
            for m in left.finditer(text):
                spans.append((m.start(), m.end()))
            for m in right.finditer(text):
                spans.append((m.start(), m.end()))

        # Bulk remove all spans
        text = dropSpans(spans, text)

        # Drop discarded elements
        for tag in options.discardElements:
            text = dropNested(text, r'<\s*%s\b[^>/]*>' % tag, r'<\s*/\s*%s>' % tag)

        if not options.toHTML:
            # Turn into text what is left (&amp;nbsp;) and <syntaxhighlight>
            text = unescape(text)

        # Expand placeholders
        for pattern, placeholder in placeholder_tag_patterns:
            index = 1
            for match in pattern.finditer(text):
                text = text.replace(match.group(), '%s_%d' % (placeholder, index))
                index += 1

        text = text.replace('<<', '«').replace('>>', '»')

        #############################################

        # Cleanup text
        text = text.replace('\t', ' ')
        text = spaces.sub(' ', text)
        text = dots.sub('...', text)
        text = re.sub(' (,:\.\)\]»)', r'\1', text)
        text = re.sub('(\[\(«) ', r'\1', text)
        text = re.sub(r'\n\W+?\n', '\n', text, flags=re.U)  # lines with only punctuations
        text = text.replace(',,', ',').replace(',.', '.')
        if options.keep_tables:
            # the following regular expressions are used to remove the wikiml chartacters around table strucutures
            # yet keep the content. The order here is imporant so we remove certain markup like {| and then
            # then the future html attributes such as 'style'. Finally we drop the remaining '|-' that delimits cells.
            text = re.sub(r'!(?:\s)?style=\"[a-z]+:(?:\d+)%;\"', r'', text)
            text = re.sub(r'!(?:\s)?style="[a-z]+:(?:\d+)%;[a-z]+:(?:#)?(?:[0-9a-z]+)?"', r'', text)
            text = text.replace('|-', '')
            text = text.replace('|', '')
        if options.toHTML:
            text = cgi.escape(text)
        return text


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
        return '%s/wiki_%02d' % (self._dirname(), self.file_index)


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

def process_dump(input_file, template_file, out_file, file_size, file_compress,
                 process_count):
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

    # collect siteinfo
    for line in input:
        # When an input file is .bz2 or .gz, line can be a bytes even in Python 3.
        if not isinstance(line, text_type): line = line.decode('utf-8')
        m = tagRE.search(line)
        if not m:
            continue
        tag = m.group(2)
        if tag == 'base':
            # discover urlbase from the xml dump file
            # /mediawiki/siteinfo/base
            base = m.group(3)
            options.urlbase = base[:base.rfind("/")]
        elif tag == 'namespace':
            mk = keyRE.search(line)
            if mk:
                nsid = mk.group(1)
            else:
                nsid = ''
            options.knownNamespaces[m.group(3)] = nsid
            if re.search('key="10"', line):
                options.templateNamespace = m.group(3)
                options.templatePrefix = options.templateNamespace + ':'
            elif re.search('key="828"', line):
                options.moduleNamespace = m.group(3)
                options.modulePrefix = options.moduleNamespace + ':'
        elif tag == '/siteinfo':
            break

    if options.expand_templates:
        # preprocess
        template_load_start = default_timer()
        if template_file:
            if os.path.exists(template_file):
                logging.info("Loading template definitions from: %s", template_file)
                # can't use with here:
                file = fileinput.FileInput(template_file,
                                           openhook=fileinput.hook_compressed)
                load_templates(file)
                file.close()
            else:
                if input_file == '-':
                    # can't scan then reset stdin; must error w/ suggestion to specify template_file
                    raise ValueError("to use templates with stdin dump, must supply explicit template-file")
                logging.info("Preprocessing '%s' to collect template definitions: this may take some time.", input_file)
                load_templates(input, template_file)
                input.close()
                input = fileinput.FileInput(input_file, openhook=fileinput.hook_compressed)
        template_load_elapsed = default_timer() - template_load_start
        logging.info("Loaded %d templates in %.1fs", len(options.templates), template_load_elapsed)

    # process pages
    logging.info("Starting page extraction from %s.", input_file)
    extract_start = default_timer()

    # Parallel Map/Reduce:
    # - pages to be processed are dispatched to workers
    # - a reduce process collects the results, sort them and print them.

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
                     args=(options, output_queue, spool_length,
                           out_file, file_size, file_compress))
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
    for page_data in pages_from(input):
        id, revid, title, ns, page = page_data
        if keepPage(ns, page):
            # slow down
            delay = 0
            if spool_length.value > max_spool_length:
                # reduce to 10%
                while spool_length.value > max_spool_length / 10:
                    time.sleep(10)
                    delay += 10
            if delay:
                logging.info('Delay %ds', delay)
            job = (id, revid, title, page, page_num)
            jobs_queue.put(job)  # goes to any available extract_process
            page_num += 1
        page = None  # free memory

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


# ----------------------------------------------------------------------
# Multiprocess support


def extract_process(opts, i, jobs_queue, output_queue):
    """Pull tuples of raw page content, do CPU/regex-heavy fixup, push finished text
    :param i: process id.
    :param jobs_queue: where to get jobs.
    :param output_queue: where to queue extracted text for output.
    """

    global options
    options = opts

    createLogger(options.quiet, options.debug)

    out = StringIO()  # memory buffer

    while True:
        job = jobs_queue.get()  # job is (id, title, page, page_num)
        if job:
            id, revid, title, page, page_num = job
            try:
                e = Extractor(*job[:4])  # (id, revid, title, page)
                page = None  # free memory
                e.extract(out)
                text = out.getvalue()
            except:
                text = ''
                logging.exception('Processing page: %s %s', id, title)

            output_queue.put((page_num, text))
            out.truncate(0)
            out.seek(0)
        else:
            logging.debug('Quit extractor')
            break
    out.close()


report_period = 10000  # progress report period


def reduce_process(opts, output_queue, spool_length,
                   out_file=None, file_size=0, file_compress=True):
    """Pull finished article text, write series of files (or stdout)
    :param opts: global parameters.
    :param output_queue: text to be output.
    :param spool_length: spool length.
    :param out_file: filename where to print.
    :param file_size: max file size.
    :param file_compress: whether to compress output.
    """

    global options
    options = opts

    createLogger(options.quiet, options.debug)

    if out_file:
        nextFile = NextFile(out_file)
        output = OutputSplitter(nextFile, file_size, file_compress)
    else:
        output = sys.stdout if PY2 else sys.stdout.buffer
        if file_compress:
            logging.warn("writing to stdout, so no output compression (use an external tool)")

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

# Minimum size of output files
minFileSize = 200 * 1024


def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("input",
                        help="XML wiki dump file")
    groupO = parser.add_argument_group('Output')
    groupO.add_argument("-o", "--output", default="text",
                        help="directory for extracted files (or '-' for dumping to stdout)")
    groupO.add_argument("-b", "--bytes", default="1M",
                        help="maximum bytes per output file (default %(default)s)",
                        metavar="n[KMG]")
    groupO.add_argument("-c", "--compress", action="store_true",
                        help="compress output files using bzip")
    groupO.add_argument("--json", action="store_true",
                        help="write output in json format instead of the default one")

    groupP = parser.add_argument_group('Processing')
    groupP.add_argument("--html", action="store_true",
                        help="produce HTML output, subsumes --links")
    groupP.add_argument("-l", "--links", action="store_true",
                        help="preserve links")
    groupP.add_argument("-s", "--sections", action="store_true",
                        help="preserve sections")
    groupP.add_argument("--lists", action="store_true",
                        help="preserve lists")
    groupP.add_argument("-ns", "--namespaces", default="", metavar="ns1,ns2",
                        help="accepted namespaces in links")
    groupP.add_argument("--templates",
                        help="use or create file containing templates")
    groupP.add_argument("--no-templates", action="store_false",
                        help="Do not expand templates")
    groupP.add_argument("-r", "--revision", action="store_true", default=options.print_revision,
                        help="Include the document revision id (default=%(default)s)")
    groupP.add_argument("--min_text_length", type=int, default=options.min_text_length,
                        help="Minimum expanded text length required to write document (default=%(default)s)")
    groupP.add_argument("--filter_disambig_pages", action="store_true", default=options.filter_disambig_pages,
                        help="Remove pages from output that contain disabmiguation markup (default=%(default)s)")
    groupP.add_argument("-it", "--ignored_tags", default="", metavar="abbr,b,big",
                        help="comma separated list of tags that will be dropped, keeping their content")
    groupP.add_argument("-de", "--discard_elements", default="", metavar="gallery,timeline,noinclude",
                        help="comma separated list of elements that will be removed from the article text")
    groupP.add_argument("--keep_tables", action="store_true", default=options.keep_tables,
                        help="Preserve tables in the output article text (default=%(default)s)")
    default_process_count = max(1, cpu_count() - 1)
    parser.add_argument("--processes", type=int, default=default_process_count,
                        help="Number of processes to use (default %(default)s)")

    groupS = parser.add_argument_group('Special')
    groupS.add_argument("-q", "--quiet", action="store_true",
                        help="suppress reporting progress info")
    groupS.add_argument("--debug", action="store_true",
                        help="print debug info")
    groupS.add_argument("-a", "--article", action="store_true",
                        help="analyze a file containing a single article (debug option)")
    groupS.add_argument("-v", "--version", action="version",
                        version='%(prog)s ' + version,
                        help="print program version")

    args = parser.parse_args()

    options.keepLinks = args.links
    options.keepSections = args.sections
    options.keepLists = args.lists
    options.toHTML = args.html
    options.write_json = args.json
    options.print_revision = args.revision
    options.min_text_length = args.min_text_length
    if args.html:
        options.keepLinks = True

    options.expand_templates = args.no_templates
    options.filter_disambig_pages = args.filter_disambig_pages
    options.keep_tables = args.keep_tables

    try:
        power = 'kmg'.find(args.bytes[-1].lower()) + 1
        file_size = int(args.bytes[:-1]) * 1024 ** power
        if file_size < minFileSize:
            raise ValueError()
    except ValueError:
        logging.error('Insufficient or invalid size: %s', args.bytes)
        return

    if args.namespaces:
        options.acceptedNamespaces = set(args.namespaces.split(','))

    # ignoredTags and discardElemets have default values already supplied, if passed in the defaults are overwritten
    if args.ignored_tags:
        ignoredTags = set(args.ignored_tags.split(','))
    else:
        ignoredTags = [
            'abbr', 'b', 'big', 'blockquote', 'center', 'cite', 'em',
            'font', 'h1', 'h2', 'h3', 'h4', 'hiero', 'i', 'kbd',
            'p', 'plaintext', 's', 'span', 'strike', 'strong',
            'tt', 'u', 'var'
        ]

    # 'a' tag is handled separately
    for tag in ignoredTags:
        ignoreTag(tag)

    if args.discard_elements:
        options.discardElements = set(args.discard_elements.split(','))

    FORMAT = '%(levelname)s: %(message)s'
    logging.basicConfig(format=FORMAT)

    options.quiet = args.quiet
    options.debug = args.debug

    createLogger(options.quiet, options.debug)

    input_file = args.input

    if not options.keepLinks:
        ignoreTag('a')

    if args.article:
        if args.templates:
            if os.path.exists(args.templates):
                with open(args.templates) as file:
                    load_templates(file)

        file = fileinput.FileInput(input_file, openhook=fileinput.hook_compressed)
        for page_data in pages_from(file):
            id, revid, title, ns, page = page_data
            Extractor(id, revid, title, page).extract(sys.stdout)
        file.close()
        return

    output_path = args.output
    if output_path != '-' and not os.path.isdir(output_path):
        try:
            os.makedirs(output_path)
        except:
            logging.error('Could not create: %s', output_path)
            return

    process_dump(input_file, args.templates, output_path, file_size,
                 args.compress, args.processes)


if __name__ == '__main__':
    main()
