import multiprocessing
import string
import operator
import glob
import ujson
import spacy
from mapreduce import MapReduce

def transform_file(filename):
    print(multiprocessing.current_process().name, 'reading', filename)
    output = []
    nlp = spacy.load('en_core_web_sm', disable=['textcat'])

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

    with bz2.BZ2File(loc) as file_:
        for i, line in enumerate(file_):
            text = ujson.loads(line)['body']
            doc = nlp(text)
            output.append(transform_doc(doc))

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
            
    return output


def write_func(items):
     with open('/home/newscred/Workspace/extractor/train/output.txt', 'a+', encoding='utf8') as f:
        for x in items:
            f.write(x)
    return

if __name__ == '__main__':
    input_files = glob.glob('train/*')
    
    mapper = MapReduce(transform_file, write_func)
    word_counts = mapper(input_files)
    
    print('Done processing!')
