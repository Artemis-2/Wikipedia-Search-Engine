from collections import defaultdict
import timeit
import xml.sax
from unidecode import unidecode
import sys
import re
import Stemmer
import heapq
import operator
import os
import pdb
import threading
from tqdm import tqdm

class Queue(object):
     
    def __init__(self):
        self.item = []
 
    def __str__(self):
        return "{}".format(self.item)
 
    def __repr__(self):
        return "{}".format(self.item)
 
    def enque(self, item):
        """
        Insert the elements in queue
        :param item: Any
        :return: Bool
        """
        self.item.insert(0, item)
        return True
    
    def peek(self):
        """
        Check the Last elements
        :return: Any
        """
        if self.size() == 0:
            return None
        else:
            return self.item[-1]

 
    def size(self):
        """
        Return the size of queue
        :return: Int
        """
        return len(self.item)
 
    def dequeue(self):
        """
        Return the elements that came first
        :return: Any
        """
        if self.size() == 0:
            return None
        else:
            return self.item.pop()



def get_plaintext(corpus):
	corpus = re.sub(r'[^A-Za-z0-9]+', r' ', corpus) #Remove Special Characters
	return corpus
##### Tokenization
def make_tokens(corpus):
    corpus = corpus.encode("ascii", errors="ignore").decode()
    corpus = get_plaintext(corpus)
    return corpus.split(' ')
##### Stop Words Removal
def remove_stopwords(text):
    return [word for word in text if stop_dict[word] != 1 ]
##### Stemming
def stem(text):
	return stemmer.stemWords(text)
    #return [stemmer.stem(word) for word in text]
##### Process Text using regex
def processText(corpus, title):
    ####  Links, Categories below references & info,body,References , tilte above references.
    
    corpus = corpus.lower() #Case Folding
    temp = corpus.split("== references == ")
    
    if len(temp) == 1:
        temp = corpus.split("==references==")
    if len(temp) == 1: # If empty then initialize with empty lists
        links = []
        categories = []
        # references = []
    else: 
        # categories = process_categories(temp[1])
        # links = process_links(temp[1])
        que11 = Queue()
        que22 = Queue()
       
        t1 = threading.Thread(target= process_categories(temp[1],que11), args=(que11,))
        t1.start()
        t2 = threading.Thread(target= process_links(temp[1],que22), args=(que22,))
        t2.start()
       
        t1.join()
        t2.join()
       
        categories = que11.peek()
        links = que22.peek()
        
              
    body = process_body(temp[0])
    title= title.lower()
    title = process_title(title)
    info = process_info(temp[0])
    references = process_ref(temp[0])
    # if references!=[] :
    #     print("name of page:",title)
    #     print("temp[1]:", temp[0])
    #     print("references:", references)
    return title, body, info, categories, links, references


def process_title(text):
    data = make_tokens(text)
    data = remove_stopwords(data)
    data = stem(data)
    return data

def process_body(text):
    data = re.sub(r'\{\{.*\}\}', r' ', text)
    data = make_tokens(data)
    data = remove_stopwords(data)
    data = stem(data)
    return data

def process_info(text):
    data = text.split('\n')
    fl = -1
    info = []
    st="}}"
    for line in data:
        if re.match(r'\{\{infobox', line):
            info.append(re.sub(r'\{\{infobox(.*)', r'\1', line))
            fl = 0
        elif fl == 0:
            if line == st:
                fl = -1
                continue
            info.append(line)
    data = make_tokens(' '.join(info))
    data = remove_stopwords(data)
    data = stem(data)
    return data

def process_categories(text,que):
    data = text.split('\n')
    categories = []
    for line in data:
        if re.match(r'\[\[category', line):
            categories.append(re.sub(r'\[\[category:(.*)\]\]', r'\1', line))
    data = make_tokens(' '.join(categories))
    data = remove_stopwords(data)
    data = stem(data)
    que.enque(data)
    # return data

def process_links(text, que):
    data = text.split('\n')
    links = []
    for line in data:
        if re.match(r'\*[\ ]*\[', line):
            links.append(line)
    data = make_tokens(' '.join(links))
    data = remove_stopwords(data)
    data = stem(data)
    que.enque(data)
    # return data

def process_ref(text):
    
        data = text.split('\n')
        refs = []
        for line in data:
            if re.search(r'<ref', line):
                refs.append(re.sub(r'.*title[\ ]*=[\ ]*([^\|]*).*', r'\1', line))
                #print("hererererere")

        data = make_tokens(' '.join(refs))
        data = remove_stopwords(data)
        data = stem(data)
        # que.enque(data)
        return data

def make_words(words, parts):
    temp = {}
    for word in parts:
        if(temp.get(word)==None):
            temp[word]=1
        else:
            temp[word]+=1
        if(words.get(word)==None):
            words[word]=1
        else:
            words[word]+=1
    return words, temp

def Indexer(title, body, info, categories, links, references):
    global no_of_pages
    global PostList
    global docID
    global no_of_files
    global offset
    ID = no_of_pages
    words={}
    # words, title = make_words(words, title)
    # words, body = make_words(words, body)
    # words, info = make_words(words, info)
    # words, categories = make_words(words,categories)
    # words, links = make_words(words, links)
    # words, references = make_words(words, references)

    d={}
    for word in links:
        if(d.get(word)==None):
            d[word]=1
        else:
            d[word]+=1
        if(words.get(word)==None):
            words[word]=1
        else:
            words[word]+=1
    links = d
    d = {} # Local Vocabulary
    for word in title:
        if(d.get(word)==None):
            d[word]=1
        else:
            d[word]+=1
        if(words.get(word)==None):
            words[word]=1
        else:
            words[word]+=1
    title = d
    d = {}
    for word in info:
        if(d.get(word)==None):
            d[word]=1
        else:
            d[word]+=1
        if(words.get(word)==None):
            words[word]=1
        else:
            words[word]+=1
    info = d
    d = {}
    for word in body:
        if(d.get(word)==None):
            d[word]=1
        else:
            d[word]+=1
        if(words.get(word)==None):
            words[word]=1
        else:
            words[word]+=1
    body = d
    d = {}
    for word in categories:
        if(d.get(word)==None):
            d[word]=1
        else:
            d[word]+=1
        if(words.get(word)==None):
            words[word]=1
        else:
            words[word]+=1
    categories = d 
    d = {}
    for word in references:
        if(d.get(word)==None):
            d[word]=1
        else:
            d[word]+=1
        if(words.get(word)==None):
            words[word]=1
        else:
            words[word]+=1
    references = d
    for word,key in words.items():
        string ='d'+(str(ID))
        if(title.get(word)):
            string += 't' + str(title[word])
        if(body.get(word)):
            string += 'b' + str(body[word])
        if(info.get(word)):
            string += 'i' + str(info[word])
        if(categories.get(word)):
            string += 'c' + str(categories[word])
        if(links.get(word)):
            string += 'l' + str(links[word])
        if(references.get(word)):
            string += 'r' + str(references[word])
        PostList[word].append(string)
    
    tem_var = no_of_pages%20000
    if tem_var == 0 :
    	offset = writeinfile(PostList, docID, no_of_files , offset)
    	PostList = defaultdict(list)
    	docID = {}
    	no_of_files = no_of_files + 1


    no_of_pages += 1




def writeinfile(posting_list, doc_id, no_of_files , offset):
    data = []
    for key in sorted(posting_list.keys()):
        substr = key + ' '
        posts = posting_list[key]
        substr +=  ' '.join(posts)
        data.append(substr)
   
    f_name = './files/index' + str(no_of_files) + '.txt'
    with open(f_name, 'a') as f:
        f.write('\n'.join(data))

    data_offset = []    
    data = []
    page_offset = offset
    for key in sorted(doc_id):
        temp = str(key) + ' ' + doc_id[key].strip()
        
        if(len(temp)>0):
            page_offset += 1  + len(temp)
        else:
            page_offset = 1 + page_offset
            

        data.append(temp)
        data_offset.append(str(page_offset))

    with open('./files/titleOffset.txt', 'a') as f:
        f.write('\n'.join(data_offset))
        f.write('\n')

    with open('./files/title.txt', 'a') as f:
        f.write('\n'.join(data))
        f.write('\n')

   
    return page_offset


   

class writeThread(threading.Thread):
    def __init__(self, field, data, offset, count):
        threading.Thread.__init__(self)
        self.fld = field
        self.offset = offset
        self.data = data
        self.cnt = count
    def run(self):
        
        f_name = './files/'  + self.fld + str(self.cnt) + '.txt'
        with open(f_name, 'w') as f:
            f.write('\n'.join(self.data))
            
        f_name = './files/offset_' + self.fld +  str(self.cnt) + '.txt'
        with open(f_name, 'w') as f:
            f.write('\n'.join(self.offset))

def final_write(data, finalCount, offsetSize):
    offset = []
    distinctWords = []
    title = defaultdict(dict)
    link = defaultdict(dict)
    info = defaultdict(dict)
    category = defaultdict(dict)
    body = defaultdict(dict)
    references = defaultdict(dict)
    for key in tqdm(sorted(data.keys())):
        
        docs = data[key]
     
        
        for i in range(0, len(docs)):
            posting = docs[i]
            # print(posting)
            
            docID = re.sub(r'.*d([0-9]*).*', r'\1', posting)

            substr = re.sub(r'.*r([0-9]*).*', r'\1', posting)
            if len(substr) >0 and posting != substr:
                references[key][docID] = substr
                # print(float(substr))
            
            substr = re.sub(r'.*l([0-9]*).*', r'\1', posting)
            if len(substr) > 0 and posting != substr:
                link[key][docID] = substr

            substr = re.sub(r'.*c([0-9]*).*', r'\1', posting)
            if len(substr)>0 and posting != substr:
                category[key][docID] = substr

            substr = re.sub(r'.*i([0-9]*).*', r'\1', posting)
            if len(substr)>0 and posting != substr:
                info[key][docID] = substr


            substr = re.sub(r'.*b([0-9]*).*', r'\1', posting)
            if len(substr)>0 and posting != substr:
                body[key][docID] = substr

            substr = re.sub(r'.*t([0-9]*).*', r'\1', posting)     
            if len(substr) >0 and posting != substr:
                title[key][docID] = substr

        string = key + ' ' + str(finalCount) + ' ' + str(len(docs))
        distinctWords.append(string)
        offset.append(str(offsetSize))
        offsetSize += 1 + len(string)

    with open( './files/offset.txt', 'a') as f:
        f.write('\n'.join(offset))
        f.write('\n')
   
    with open( './files/vocab.txt', 'a') as f:
        f.write('\n'.join(distinctWords))
        f.write('\n')
       
    c_data, c_offset = [],[]
    b_data, b_offset= [],[]
    l_data, l_offset  = [],[]
    r_data, r_offset = [],[]
    infoData, infoOffset = [], []
    titleData,  titleOffset = [], []
    t_prev, i_prev, r_prev ,b_prev, c_prev, l_prev  = 0, 0, 0, 0, 0, 0  

    for key in tqdm(sorted(data.keys())):
        if key in link:
            occurences = link[key]
            occurences = sorted(occurences, key = occurences.get, reverse=True)
            l_offset.append(str(l_prev) + ' ' + str(len(occurences)))
            string = key + ' '
            for doc in occurences:
                string += doc + ' ' + str(float(link[key][doc])) + ' '
            l_data.append(string)
            l_prev += 1 + len(string)

        if key in category:
            occurences = category[key]
            occurences = sorted(occurences, key = occurences.get, reverse=True)
            c_offset.append(str(c_prev) + ' ' + str(len(occurences)))
            string = key + ' '
            for doc in occurences:
                string += doc + ' ' + str(float(category[key][doc])) + ' '
            c_data.append(string)
            c_prev += 1 + len(string)

        if key in references:
            occurences = references[key]
            occurences = sorted(occurences, key = occurences.get, reverse = True)
            r_offset.append(str(r_prev) + ' ' + str(len(occurences)))
            string = key + ' '
            for doc in occurences:
                string += doc + ' ' + str(float(references[key][doc])) + ' '
            r_data.append(string) 
            r_prev += 1 + len(string)

        if key in info:
            occurences = info[key]
            occurences = sorted(occurences, key = occurences.get, reverse=True)
            infoOffset.append(str(i_prev) + ' ' + str(len(occurences)))
            string = key + ' '
            for doc in occurences:
                string += doc + ' ' + str(float(info[key][doc])) + ' '
            infoData.append(string)
            i_prev += 1 + len(string)

        if key in body:
            occurences = body[key]
            occurences = sorted(occurences, key = occurences.get, reverse=True)
            b_offset.append(str(b_prev) + ' ' + str(len(occurences)))
            string = key + ' '
            for doc in occurences:
                string += doc + ' ' + str(float(body[key][doc])) + ' '
            b_data.append(string)
            b_prev += 1 + len(string)
        
        if key in title:
            occurences = title[key]
            occurences = sorted(occurences, key = occurences.get, reverse=True)
            titleOffset.append(str(t_prev) + ' ' + str(len(occurences)))
            string = key + ' '
            for doc in occurences:
                string += doc + ' ' + str(float(title[key][doc])) + ' '
            titleData.append(string)
            t_prev += 1 + len(string)
        
    tasks = []
    tasks.append(writeThread('r', r_data, r_offset, finalCount))
    tasks.append(writeThread('t', titleData, titleOffset, finalCount))
    tasks.append(writeThread('b', b_data, b_offset, finalCount))
    tasks.append(writeThread('i', infoData, infoOffset, finalCount))
    tasks.append(writeThread('c', c_data, c_offset, finalCount))
    tasks.append(writeThread('l', l_data, l_offset, finalCount))
    
   
    for i in range(0,6):
        tasks[i].start()
        
    
    for i in range(0,6):
        tasks[i].join()
        
   
    
    return offsetSize , finalCount+1
  


def mergefiles(fileCount):
    global total_tokens
    flag = []
    for j in range(fileCount):
        flag.append(0)
  
    file_ptr = {}
    words = {}
    word_heap = []
    finalCount,offsetSize = 0,0
   
    first_line = {}
    data = defaultdict(list)
    

    for i in range(0, fileCount):
  
        f_name = './files/index'  + str(i) + '.txt'
        file_ptr[i] = open(f_name, 'r')
        first_line[i] = file_ptr[i].readline().strip()
        words[i] = first_line[i].split()
        if  words[i][0] not in word_heap:
            heapq.heappush(word_heap, words[i][0])
        flag[i] = 1
        
    count = 1
    while any(flag) == 1:

        
        smal_word = heapq.heappop(word_heap)

        if count%100000 == 0:
            oldFileCount = finalCount
            offsetSize,finalCount = final_write(data, finalCount, offsetSize)
            if finalCount != oldFileCount :
                data = defaultdict(list)
        
        count += 1
        total_tokens += 1
        
        for i in range(0, fileCount):

            if flag[i]:

                if smal_word == words[i][0] :
                    
                    first_line[i] = file_ptr[i].readline().strip()
                    data[smal_word].extend(words[i][1:])
                    if first_line[i] != '':
                        words[i] = first_line[i].split()
                        if words[i][0] not in word_heap:
                            heapq.heappush(word_heap, words[i][0])
                    else:
                        flag[i] = 0 # we have written this line
                        file_ptr[i].close()       
            # i+=1            
    offsetSize,finalCount = final_write(data, finalCount, offsetSize)
    
class Handle(xml.sax.ContentHandler):
    def __init__(self):
        self.title = ''
        self.text = ''
        self.data = ''
        self.ID = ''
        self.flag = 1
    def startElement(self, tag, attributes):
        self.data = tag   
    def endElement(self, tag):
        if tag == 'page':
            docID[no_of_pages] = self.title.strip().encode("ascii", errors="ignore").decode()
            title, body, info, categories, links, references = processText(self.text, self.title)
            Indexer( title, body, info, categories, links, references)
            self.text = ''
            self.data = ''
            self.title = ''
            self.ID = ''
            self.flag = 1
            #print('Finished:',no_of_pages)
    def characters(self, content):
        if self.data == 'title':
            self.title = self.title + content
        elif self.data == 'id' and self.flag == 1:
            self.flag = 0
            self.ID = content    
        elif self.data == 'text':
            self.text += content

class Parser():
    def __init__(self, file_input):
        self.parser = xml.sax.make_parser()
        self.parser.setFeature(xml.sax.handler.feature_namespaces, 0)
        self.handler = Handle()
        self.parser.setContentHandler(self.handler)
        self.parser.parse(file_input)

if __name__ == '__main__':
    ##### Store stop words in a dictionary
    with open('./files/stopwords.txt', 'r') as file :
    	stop_words = set(file.read().split('\n'))

    stop_dict = defaultdict(int)

    for word in stop_words:
    	stop_dict[word] = 1

    docID = {} ## Dictionary: {Doc id : Title}
    no_of_pages = 0 ### Page Count
    no_of_files = 0 ### File Count
    offset = 0 ## Offset
    PostList = defaultdict(list)

    ##### Initialise Porter Stemmer
    #stemmer = PorterStemmer() 
    stemmer = Stemmer.Stemmer('english')

    ##### Begin Parsing
    for filename in os.listdir(sys.argv[1]):
        print(filename)
        parser = Parser(sys.argv[1]+'/'+filename)
    
    with open('./files/fileNumbers.txt', 'w') as f:
        f.write(str(no_of_pages))

    
    #To write out remaining pages %20000
    offset = writeinfile(PostList, docID, no_of_files , offset)
    PostList = defaultdict(list)
    docID = {}
    total_tokens = 0
    no_of_files = no_of_files + 1
    mergefiles(no_of_files)
    del_ctr = 0
    while os.path.exists("./files/index"+ str(del_ctr) + ".txt"):
        os.remove("./files/index"+ str(del_ctr) + ".txt")
        del_ctr +=1
    siz = os.path.getsize("./files")
    with open('./files/stats.txt', 'w') as f:
        f.write(str(siz) + "\n" + str(no_of_files) + "\n" + str(total_tokens))
    