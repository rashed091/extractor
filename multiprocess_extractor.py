from __future__ import print_function, unicode_literals, division
import io
import bz2
from toolz import partition
from os import path
import multiprocessing
import os
import re
import ujson
import random
import spacy
import argparse
import fileinput
from itertools import zip_longest

import sys
from joblib import Parallel, delayed

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


def parallelize(func, iterator, n_jobs, extra):
    extra = tuple(extra)
    return Parallel(n_jobs=n_jobs)(delayed(func)(*(item + extra)) for item in iterator)


def iter_comments(loc):
    with bz2.BZ2File(loc) as file_:
        for i, line in enumerate(file_):
            yield ujson.loads(line)['body']


pre_format_re = re.compile(r'^[\`\*\~]')
post_format_re = re.compile(r'[\`\*\~]$')
url_re = re.compile(r'\[([^]]+)\]\(%%URL\)')
link_re = re.compile(r'\[([^]]+)\]\(https?://[^\)]+\)')


def strip_meta(text):
    text = link_re.sub(r'\1', text)
    text = text.replace('&gt;', '>').replace('&lt;', '<')
    text = pre_format_re.sub('', text)
    text = post_format_re.sub('', text)
    return text


def parse_and_transform(batch_id, input_, out_dir):
    out_loc = path.join(out_dir, '%d.txt' % batch_id)
    if path.exists(out_loc):
        return None
    print('Batch', batch_id)
    nlp = spacy.load('en_core_web_sm')
    with io.open(out_loc, 'w', encoding='utf8') as file_:
        for text in input_:
            try:
                doc =nlp(strip_meta(text))
                file_.write(transform_doc(doc))
            except Exception as e:
                print(strip_meta(text))
                print('Error occured here?')


def worker(input_):
    batch_id = random.randint(0, 1000)
    out_loc = path.join('Reddit', '%d.txt' % batch_id)
    if path.exists(out_loc):
        return None
    print('Batch', batch_id)
    nlp = spacy.load('en_core_web_sm')
    with io.open(out_loc, 'w', encoding='utf8') as file_:
        for text in input_:
            try:
                doc =nlp(strip_meta(text))
                file_.write(transform_doc(doc))
            except Exception as e:
                print(strip_meta(text))
                print('Error occured here?')


def transform_doc(doc):
    for ent in doc.ents:
        ent.merge(tag=ent.root.tag_, lemma=ent.text, ent_type=LABELS[ent.label_])
    for np in doc.noun_chunks:
        while len(np) > 1 and np[0].dep_ not in ('advmod', 'amod', 'compound'):
            np = np[1:]
        np.merge(tag=np.root.tag_, lemma=np.text, ent_type=np.root.ent_type_)
    strings = []
    for sent in doc.sents:
        if sent.text.strip():
            strings.append(' '.join(represent_word(w) for w in sent if not w.is_space))
    if strings:
        return '\n'.join(strings) + '\n'
    else:
        return ''


def represent_word(word):
    if word.like_url:
        return '%%URL|X'
    text = re.sub(r'\s', '_', word.text)
    tag = LABELS.get(word.ent_type_, word.pos_)
    if not tag:
        tag = '?'
    return text + '|' + tag

def text_from(file_):
    comments = []
    for i, line in enumerate(file_):
        comments.append(ujson.loads(line)['body'])
    return comments

def grouper(n, iterable, padvalue=None):
    return zip_longest(*[iter(iterable)]*n, fillvalue=padvalue)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__doc__)
    parser.add_argument("input", help="XML wiki dump file")

    args = parser.parse_args()

    input_file = args.input

    processes = multiprocessing.Pool(8)
    #
    # input = fileinput.FileInput(input_file, openhook=fileinput.hook_compressed)
    #
    # batches = text_from(input)

    groups = partition(100000, iter_comments(input_file))

    for g in groups:
        result = processes.map(worker, [g])
    
