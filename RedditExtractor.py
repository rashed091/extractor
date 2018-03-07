from __future__ import print_function, unicode_literals, division
import io
import bz2
from toolz import partition
from os import path
import re
import ujson
import spacy
from timeit import default_timer
from joblib import Parallel, delayed
from multiprocessing import cpu_count

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


def parse_and_transform(batch_id, input_, out_dir):
    out_loc = path.join(out_dir, '%d.txt' % batch_id)
    if path.exists(out_loc):
        return None
    print('Batch', batch_id)
    nlp = spacy.load('en_core_web_sm', disable=['textcat'])
    with io.open(out_loc, 'w', encoding='utf8') as file_:
        for text in input_:
            try:
                t1 = default_timer()
                doc =nlp(strip_meta(text))
                print('Spacy: {}'.format(100 * (default_timer() - t1)))
                t1 = default_timer()
                file_.write(transform_doc(doc))
                print('Write: {}'.format(100 * (default_timer() - t1)))
            except Exception as e:
                print('Error! {}'.format(strip_meta(text)))


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


def main(in_loc='/home/newscred/Workspace/extractor/data/RC_2007-12.bz2', out_dir="/home/newscred/Workspace/extractor/data"):
    if not path.exists(out_dir):
        path.join(out_dir)
    t1 = default_timer()
    jobs = partitiona_all(10000, iter_comments(in_loc))
    do_work = parse_and_transform
    worker = cpu_count() - 2
    print('Number of worker# {}'.format(worker))
    parallelize(do_work, enumerate(jobs), worker, [out_dir])
    t2 = default_timer()
    print('Execution Time: {}'.format(t2-t1))


if __name__ == '__main__':
    main()
