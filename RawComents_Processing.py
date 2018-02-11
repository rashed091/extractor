from __future__ import print_function, unicode_literals, division
import io
import bz2
from toolz import partition
from os import path
import re
import ujson
import spacy
import time
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
    nlp = spacy.load('en')
    with io.open(out_loc, 'w', encoding='utf8') as file_:
        for text in input_:
            try:
                doc =nlp(strip_meta(text))
                file_.write(transform_doc(doc))
            except Exception as e:
                print('Error! {}'.format(strip_meta(text)))


def test_parse_and_transform(text):
    nlp = spacy.load('en')
    try:
        doc =nlp(strip_meta(text))
        print('Original -> {}'.format(text))
        print('Formated -> {}'.format(transform_doc(doc)))
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


def batch_process(in_loc='D:\Workspace\sense2vec\data\RC_2009-01.bz2', out_dir="D:\Workspace\sense2vec\data", n_workers=4):
    if not path.exists(out_dir):
        path.join(out_dir)
    jobs = partition(50000, iter_comments(in_loc))
    do_work = parse_and_transform
    parallelize(do_work, enumerate(jobs), n_workers, [out_dir])


def main(in_loc='/Users/newscred/Workspace/extractor/data/RC_2008-01.bz2', out_dir="/Users/newscred/Workspace/extractor/data", n_workers=4, load_parses=False):
    if not path.exists(out_dir):
        path.join(out_dir)

    t1 = time.time()
    batches = partition(50000, iter_comments(in_loc))
    for i, batch in enumerate(batches):
        parse_and_transform(i, batch, out_dir)
        print('Batch# {} is completed!'.format(i))
    t2 = time.time()
    print("Total time: %.3f" % (t2 - t1))


if __name__ == '__main__':
    test_parse_and_transform('''Wow, you're a buzz-kill.''')
    test_parse_and_transform('''Time to hang out with a different crowd there, 1smartass.''')
    test_parse_and_transform('''Indeed I did, but it was intentional.  \n\nThat clock watching was some good stuff. I've saturated my inner thigh with my man fat.''')
    test_parse_and_transform('''I FUCKING HATE DIGG MAN,,,\r\n\r\n\r\n\"IM SORRY YOUR SESSION HAS TIMED OUT\" AND OUR WEBSITE EATS DOG SHIT. FUCK DIGG BRO","score_hidden''')
    test_parse_and_transform('''Wait, what? \n\nYou're telling me that the defense is that a command window is essentially a DOS compatibility layer?  And that this is so damned hairy that they had to write a separate OS subsystem to handle it?\n\nAnd that's okay?\n\nI'm not sure what you mean by \"glorified telnet.\"  Wait, let me open up a Terminal here.  This is OS X, but it might as well be Linux or Solaris.  Essentially I'm running bash here.  It's a program.  it doesn't run in any kind of crazy subsystem.  It doesn't require separate security.  It doesn't act as sume kind of emulator or compatibility layer,  It's just a program running in a window.  \n\nLook, I'm not going to tell anyone what they should or should not like.  So if you like cmd.exe and think that's okay, then rock out.  \n\nHowever, I refuse to accept this shit.  I don't use Windows (or MS software at all where I can avoid it) because of decisions like this.  MS decides to make command windows be DOS compatibility layers, which causes side-effects like not being able to theme the window or not being able to drop objects onto it.  \n\nThen, rather than fix those problems, they tell you that the faults are by design and you can just suck it up and like it.\n\nYeah, fuck that.''')



