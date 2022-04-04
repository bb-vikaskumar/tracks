import random
import json
import math
import os

FILE_DIR = os.path.dirname(os.path.realpath(__file__)) 
TOTAL_DOCS = 1000000
QUERIES = 50000
SKIP_LINES = int(TOTAL_DOCS / QUERIES)
print('[REQ] Total Docs: ', TOTAL_DOCS)
print('[REQ] Distinct Quries: ', QUERIES)

def writeFile(fileName, data):
    print('Writing ', fileName)
    f = open(FILE_DIR+'/'+fileName, "w")
    f.write(data)
    f.close()


def getSavedDocuments():
    print('Reading saved docs from camp_only-documents.json')
    f = open(FILE_DIR+'/camp_only-documents.json', 'r')
    # data =  [json.loads(line) for line in f.readlines() if line]
    data = []
    while True:
        line = f.readline()
        if not line:
            break
        data.append(json.loads(line))
        for i in range(1, SKIP_LINES):
            f.readline()
    f.close()
    return data

def generateCombination(values, size):
    if not values or size >= len(values):
        return values

    comb = set()
    while len(comb) < size:
        comb.add(values[random.randint(0, len(values)-1)])

    return list(comb)

def getSearchQueries(docs, termSearchCount):
    queries = []
    
    for i in range(0, len(docs), 1):
        normalDoc = docs[i]

        query = {
            'query': {
                'bool': {
                    'filter': []
                }
            },
            "_source": ["camp_info"]
        }

        for field in normalDoc.keys():
            if(field == 'mid' or field == 'camp_info' or field == 'campaign_id'):
                continue

            values = generateCombination(normalDoc[field], termSearchCount[field])
            if(field != 'dc' and field != 'c_sku_id' and field != 'mtype' and field != 'status'):
                values = list(set( ['all'] + values ))
            
            terms = { field: values }

            query['query']['bool']['filter'].append({'terms': terms})

        # queries.append(query)
        docs[i] = query

    return docs

termSearchCount = {
    'mtype': 1,
    'dc': 1,
    'ds': 1,
    'bbstar': 1,
    'status': 1,
    'source': 1,
    'emails': 1,
    'phone_numbers': 1,
    'cp': 1,
    'entry_context': 1,
    'sa_city_ids': 1,
    'sa_ids': 1,
    'mid': 1,
    'campaign_id': 1,
    'c_sku_id': 1,
    'c_brand': 1,
    'c_tlc': 1,
    'c_mlc': 1,
    'c_llc': 1,
    'c_group': 1,
    'r_sku_id': 1,
    'r_brand': 1,
    'r_tlc': 1,
    'r_mlc': 1,
    'r_llc': 1,
    'r_group': 1,
}

# import documents
savedDocs = getSavedDocuments()
print('[ACTUAL] Total Docs: ', len(savedDocs))
queries = getSearchQueries(savedDocs, termSearchCount)
print('[ACTUAL] Distinct Quries: ', len(queries))
# save sample document
writeFile('sampleDocument.json', json.dumps(savedDocs[0]))
# save sample query
writeFile('sampleQuery.json', json.dumps(queries[0]))

# print('** savedDocs: ', len(savedDocs), '\n', savedDocs[0])
# print('** queries: ', len(queries), '\n', queries[0])

class TermParamSource:
    def __init__(self, track, params, **kwargs):
        print("initialized | #savedDocs: ", len(savedDocs), " | #queries: ", len(queries))
        # choose a suitable index: if there is only one defined for this track
        # choose that one, but let the user always override index and type.
        if len(track.indices) == 1:
            default_index = track.indices[0].name
            if len(track.indices[0].types) == 1:
                default_type = track.indices[0].types[0].name
            else:
                default_type = None
        else:
            default_index = "_all"
            default_type = None

        # we can eagerly resolve these parameters already in the constructor...
        self._index_name = params.get("index", default_index)
        self._type_name = params.get("type", default_type)
        self._cache = params.get("cache", False)
        # ... but we need to resolve "profession" lazily on each invocation later
        self._params = params
        # Determines whether this parameter source will be "exhausted" at some point or
        # Rally can draw values infinitely from it.
        self.infinite = True
        self._srNo = 0

    def partition(self, partition_index, total_partitions):
        # print("partition | srNo: ", self._srNo, " | savedDocs: ", len(savedDocs))
        return self

    def params(self):
        # print("params | srNo: ", self._srNo % len(savedDocs), " | savedDocs: ", len(savedDocs))
        self._srNo = self._srNo + 1

        return {
            "body": queries[self._srNo % len(savedDocs)],
            "index": self._index_name,
            "type": self._type_name,
            "cache": self._cache
        }


def register(registry):
    registry.register_param_source("my-custom-term-param-source", TermParamSource)