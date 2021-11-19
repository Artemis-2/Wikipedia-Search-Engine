import math
from collections import defaultdict
from random import randint
import linecache
import re
import timeit
import Stemmer

import sys
# from attr._make import fields

##### Stop Words Removal
def remove_stopwords(corpus):
    return [word for word in corpus if stop_dict[word] != 1 ]

##### Stemming
def stem(corpus):
    return stemmer.stemWords(corpus)

def get_plaintext(corpus):
    corpus = re.sub(r'[^A-Za-z0-9]+', r' ', corpus) #Remove Special Characters
    return corpus

##### Tokenization
def get_tokens(corpus):
    corpus = corpus.encode("ascii", errors="ignore").decode()
    corpus = get_plaintext(corpus)
    return corpus.split()



def find_numfile(offset, word, high,f, typ='str'):
    low = 0 
    while True:
        if low<high :
            mid = int((low + high) / 2)
            f.seek(offset[mid])
            wordPtr = f.readline().strip().split(' ')
            if typ != 'int':
                if wordPtr[0] == word :
                    return mid,wordPtr[1:]

                elif word > wordPtr[0]:
                    low = mid + 1 
                  

                else:
                    high = mid
                     
            else:
                if int(wordPtr[0]) == int(word):
                    return mid,wordPtr[1:]

                elif int(word) >int(wordPtr[0]):
                    
                    low = mid + 1
                else:
                    high = mid
                    
                
        else :
            return -1,[]

    return -1,[]





def doc_find( word,fileNo, field ,fieldFile):
    fieldOffset,docFreq = [],[]
   
    with open('./files/offset_' + field + fileNo + '.txt') as f:
        for line in f:
            offset, df = line.strip().split(' ')
            docFreq.append(int(df))
            fieldOffset.append(int(offset))
    mid,docList = find_numfile(fieldOffset, word, len(fieldOffset), fieldFile)
    return docFreq[mid],docList


def ranking(nfiles, qtype, results, docFreq):
    if(qtype=='f'):
        weightage = [0.10, 0.10,0.40,0.10,0.40,0.10] #r, l,b,i,t,c
    else:
        weightage = [0.05,0.05,0.40,0.05,0.40,0.10] #r,l,b,i,t,c
        
    queryIdf = {}
    docs = defaultdict(float)

    for key in docFreq:
        queryIdf[key] = math.log((float(nfiles) - float(docFreq[key]) + 0.5) / ( float(docFreq[key]) + 0.5))
        docFreq[key] = math.log((float(nfiles) / float(docFreq[key])))
    temp_dict = {'r':0, 'l':1, 'b':2 , 'i':3, 't':4, 'c':5}
    for word in results:
        fieldWisePostingList = results[word]
        # x = 0
        for field in fieldWisePostingList:
            
            if len(field) > 0:
                
                postingList = fieldWisePostingList[field]
                # if field in ['t', 'b', 'i', 'c', 'l', 'r']:
                #     factor = temp_dict[field]
                if field == 'r':
                    factor = weightage[0]
                if field == 'l':
                    factor = weightage[1]
                if field == 'b':
                    factor = weightage[2]
                if field == 'i':
                    factor = weightage[3]
                if field == 't':
                    factor = weightage[4]
                if field == 'c':
                    factor = weightage[5]
                
                # x +=1
                
                i=0
                while i < len(postingList):
                    tem = (1+math.log(float(postingList[i+1]))) * docFreq[word]
                    docs[postingList[i]] = docs[postingList[i]] + float( tem * factor )
                    i+=2
    return docs

def begin_search():
    
    
    with open('./files/titleOffset.txt', 'r') as f:
        for line in f:
            titleOffset.append(int(line.strip(' ')))

   
    with open('./files/offset.txt', 'r') as f:
        for line in f:
            offset.append(int(line.strip(' ')))

    f = open('./files/fileNumbers.txt', 'r')
    nfiles = int(f.read().strip())
    f.close()

    titleFile = open('./files/title.txt', 'r')
    fvocab = open('./files/vocab.txt', 'r')

    a = 0
    while a==0:
        a = 1
        all_queries = []
        with open(sys.argv[1], 'r') as f:
            for line in f:
                all_queries.append(line.strip(' '))
        sys.stdout = open('output.txt', 'w')
        for query in all_queries:
            query = query.lower()
            tmp = query.split(",", 1)
            K = int(tmp[0])
            query = tmp[1]
            start = timeit.default_timer()
            if re.match(r'[t|b|i|c|l|r]:', query):
                tempFields = re.findall(r'([t|b|c|i|l|r]):', query)
                words = re.findall(r'[t|b|c|i|l|r]:([^:]*)(?!\S)', query)
                # print(tempFields)
                # print(words)
                fields,tokens = [],[]
            
                
                for i in range(0, len(words)):
                    for word in words[i].split():
                        fields.append(tempFields[i])
                        tokens.append(word)
                
                tokens = remove_stopwords(tokens)
                tokens = stem(tokens)
                qtype = 'f'
                
            else:
                tokens = get_tokens(query)
                tokens = remove_stopwords(tokens)
                tokens = stem(tokens)
                fields = ['t', 'b', 'i', 'c', 'l', 'r']
                qtype = 's'

            docFreq = {}
                
            docList = defaultdict(dict)
            for word in tokens:
                _,docs = find_numfile(offset, word, len(offset), fvocab)
                if len(docs) > 0:
                    fileNo = docs[0]
                    docFreq[word] = docs[1]
                    for field in fields:
                        fieldFile = open('./files/' + field + str(fileNo) + '.txt', 'r')
                        _,returnedList = doc_find( word,fileNo, field, fieldFile)
                        docList[word][field] = returnedList
            results = docList
            results = ranking(nfiles, qtype ,results, docFreq)

            # print('\nRelevant Results:')
            #print(query)
            if len(results) > 0:
                results = sorted(results, key=results.get, reverse=True)
                results = results[:K]
                resultid = results
                # print(results)
                
                i = 0
                
              
                for key in results:
                    _,title = find_numfile(titleOffset, key,len(titleOffset), titleFile, 'int')
                    
                    print(resultid[i], " ", ' '.join(title))
                   
                
                
            end = timeit.default_timer()
            print((start - end),",", (start-end)/K)
            # print()
            # print('Time =', end-start)


if __name__ == '__main__':
	### Stop Words
    with open('./files/stopwords.txt', 'r') as file :
        stop_words = set(file.read().split('\n'))
    stop_dict = defaultdict(int)
    
    for word in stop_words:
        stop_dict[word] = 1
    offset,titleOffset = [],[]
    stemmer = Stemmer.Stemmer('english')
    begin_search()
