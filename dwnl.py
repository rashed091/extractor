from __future__ import print_function, unicode_literals, division

import random
import spacy
import argparse
import bz2
import io
import multiprocessing
import os
import re
import ujson
import sys
from itertools import zip_longest
from os import path
from toolz import partition
from multiprocessing import Pool

########################################################################

class MultiProcExtractor(object):
    #----------------------------------------------------------------------
    def __init__(self, batches, out_dir):
        """ Initialize class with list of urls """
        self.batches = batches
        self.out_dir = out_dir
        self.LABELS = {
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

    #----------------------------------------------------------------------
    def run(self):
        jobs = []
        i = 0
        for batch in self.batches:
            process = multiprocessing.Process(target=self.parse_and_transform, args=(i, batch))
            i += 1
            jobs.append(process)
            process.start()
        for job in jobs:
            job.join()

    def run_pools(self):
        worker = 3
        chunksize = 100000
        with Pool(processes=worker) as pool:
            result = pool.map(self.worker, self.batches, chunksize)

    #----------------------------------------------------------------------
    def strip_meta(self, text):
        pre_format_re = re.compile(r'^[\`\*\~]')
        post_format_re = re.compile(r'[\`\*\~]$')
        url_re = re.compile(r'\[([^]]+)\]\(%%URL\)')
        link_re = re.compile(r'\[([^]]+)\]\(https?://[^\)]+\)')
        text = link_re.sub(r'\1', text)
        text = text.replace('&gt;', '>').replace('&lt;', '<')
        text = pre_format_re.sub('', text)
        text = post_format_re.sub('', text)
        return text

    def worker(self, input_):
        num = random.randint(1, 1000)
        out_loc = path.join(self.out_dir, 'reddit-{}.txt'.format(num))
        if path.exists(out_loc):
            return None
        print('Batch', num)
        nlp = spacy.load('en_core_web_sm')
        with io.open(out_loc, 'w', encoding='utf8') as file_:
            for text in input_:
                try:
                    doc = nlp(self.strip_meta(text))
                    file_.write(self.transform_doc(doc))
                except Exception as e:
                    print(self.strip_meta(text))
                    print('Error occured here?')

    def parse_and_transform(self, batch_id, input_):
        out_loc = path.join(self.out_dir, 'reddit-{}.txt'.format(batch_id))
        if path.exists(out_loc):
            return None
        print('Batch', batch_id)
        nlp = spacy.load('en_core_web_sm')
        with io.open(out_loc, 'w', encoding='utf8') as file_:
            for text in [input_]:
                try:
                    doc = nlp(self.strip_meta(text))
                    file_.write(self.transform_doc(doc))
                except Exception as e:
                    print(self.strip_meta(text))
                    print('Error occured here?')

    def transform_doc(self, doc):
        for ent in doc.ents:
            ent.merge(tag=ent.root.tag_, lemma=ent.text, ent_type=self.LABELS[ent.label_])
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
        tag = self.LABELS.get(word.ent_type_, word.pos_)
        if not tag:
            tag = '?'
        return text + '|' + tag

#----------------------------------------------------------------------

def iter_comments(loc):
    data = []
    with bz2.BZ2File(loc) as file_:
        for i, line in enumerate(file_):
            try:
                data.append(ujson.loads(line)['body'])
            except:
                pass
    return data


def chunker(data, chunks):
    length = len(data)
    chunk_size = int(length / chunks)
    for x in range(0, length, chunk_size):
        yield data[x:x+chunk_size]

def grouper(n, iterable, padvalue=None):
    return zip_longest(*[iter(iterable)]*n, fillvalue=padvalue)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("input",
                        help="XML wiki dump file")
    groupO = parser.add_argument_group('Output')
    groupO.add_argument("-o", "--output", default="text",
                        help="directory for extracted files (or '-' for dumping to stdout)")

    args = parser.parse_args()

    input_file = args.input
    output_path = args.output

    if output_path != '-' and not os.path.isdir(output_path):
        try:
            os.makedirs(output_path)
        except:
            pass

    # batches = partition(1000000, iter_comments(input_file))
    # data = iter_comments(input_file)
    groups = partition(1000000, iter_comments(input_file))
    # print(len(data))
    extractor = MultiProcExtractor(groups, output_path)
    extractor.run()
