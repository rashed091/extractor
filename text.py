from __future__ import unicode_literals, division

import spacy
import re
import sys
import bz2
import fileinput
import os.path
import json
import ujson


# Matches space
spaces = re.compile(r' {2,}')

# Matches dots
dots = re.compile(r'\.{4,}')

pre_format_re = re.compile(r'^[\`\*\~]')
post_format_re = re.compile(r'[\`\*\~]$')
url_re = re.compile(r'\[([^]]+)\]\(%%URL\)')
link_re = re.compile(r'\[([^]]+)\]\(https?://[^\)]+\)')

## PARAMS ####################################################################

options = {
    'write_json': True
}

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

# ======================================================================

class Extractor(object):
    """
    An extraction task on a article.
    """
    def __init__(self, text):
        self.text = text
        self.nlp = spacy.load('en')

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

# --------------------------------
def text_from(file_):
    comments = []
    for i, line in enumerate(file_):
        comments.append(ujson.loads(line)['body'])
    return comments


def main():
    input_file = '/Users/newscred/Workspace/extractor/data/RC_2006-01.bz2'

    file = fileinput.FileInput(input_file, openhook=fileinput.hook_compressed)
    for comment in text_from(file):
        Extractor(comment).extract(sys.stdout)
    file.close()


if __name__ == '__main__':
    main()
