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
def strip_meta(text):
    pre_format_re = re.compile(r'^[\`\*\~]')
    post_format_re = re.compile(r'[\`\*\~]$')
    url_re = re.compile(r'\[([^]]+)\]\(%%URL\)')
    link_re = re.compile(r'\[([^]]+)\]\(https?://[^\)]+\)')

    text = link_re.sub(r'\1', text)
    text = text.replace('&gt;', '>').replace('&lt;', '<')
    text = pre_format_re.sub('', text)
    text = post_format_re.sub('', text)
    return text

def parse_and_transform(loc, out_dir, file_num):
    with bz2.BZ2File(loc) as file_:
        out_loc = path.join(out_dir, '%d.txt' % file_num)
        if path.exists(out_loc):
            return None
        with io.open(out_loc, 'w', encoding='utf8') as file_:
            for i, line in enumerate(file_):
                try:
                    text = ujson.loads(line)['body']
                    text = strip_meta(text).strip()
                    file_.write(transform_doc(doc))
                except:
                    pass


def text_from(file_):
    comments = []
    for i, line in enumerate(file_):
        try:
            text = ujson.loads(line)['body']
            comments.append(str(text).strip())
        except:
            pass
    return comments

# ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("input",
                        help="XML wiki dump file")
    groupO = parser.add_argument_group('Output')
    groupO.add_argument("-o", "--output", default="text",
                        help="directory for extracted files (or '-' for dumping to stdout)")

    default_process_count = max(1, cpu_count() - 2)
    print(default_process_count)
    parser.add_argument("--processes", type=int, default=default_process_count,
                        help="Number of processes to use (default %(default)s)")

    args = parser.parse_args()
    input_file = args.input

    output_path = args.output
    if output_path != '-' and not os.path.isdir(output_path):
        try:
            os.makedirs(output_path)
        except:
            logging.error('Could not create: %s', output_path)
            return
    parse_and_transform(input_file, output_path, 9)

if __name__ == '__main__':
    main()
