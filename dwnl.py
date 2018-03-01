from __future__ import print_function, unicode_literals, division

import requests
import spacy
import argparse
import bz2
import io
import multiprocessing
import os
import re
import ujson
import sys
from os import path
from toolz import partition



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

pre_format_re = re.compile(r'^[\`\*\~]')
post_format_re = re.compile(r'[\`\*\~]$')
url_re = re.compile(r'\[([^]]+)\]\(%%URL\)')
link_re = re.compile(r'\[([^]]+)\]\(https?://[^\)]+\)')
########################################################################

class MultiProcExtractor(object):
    #----------------------------------------------------------------------
    def __init__(self, batches):
        """ Initialize class with list of urls """
        self.batches = batches

    #----------------------------------------------------------------------
    def run(self):
        """
        Download the urls and waits for the processes to finish
        """
        jobs = []
        for batch in self.batches:
            process = multiprocessing.Process(target=self.worker, args=(batch,))
            jobs.append(process)
            process.start()
        for job in jobs:
            job.join()

    #----------------------------------------------------------------------
    def strip_meta(self, text):
        text = link_re.sub(r'\1', text)
        text = text.replace('&gt;', '>').replace('&lt;', '<')
        text = pre_format_re.sub('', text)
        text = post_format_re.sub('', text)
        return text

    def parse_and_transform(self, batch_id, input_, out_dir):
        out_loc = path.join(out_dir, '%d.txt' % batch_id)
        if path.exists(out_loc):
            return None
        print('Batch', batch_id)
        nlp = spacy.load('en_core_web_sm')
        with io.open(out_loc, 'w', encoding='utf8') as file_:
            for text in input_:
                try:
                    doc = nlp(self.strip_meta(text))
                    file_.write(self.transform_doc(doc))
                except Exception as e:
                    print(self.strip_meta(text))
                    print('Error occured here?')

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

#----------------------------------------------------------------------

def iter_comments(loc):
    with bz2.BZ2File(loc) as file_:
        for i, line in enumerate(file_):
            yield ujson.loads(line)['body']


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

    batches = partition(1000000, iter_comments(input_file))

    print(len(batches))
    # extractor = MultiProcExtractor(batches)
    # extractor.run()