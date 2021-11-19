# Wikipedia-Search-Engine
This repository consists of a search engine over the Wikipedia dump. The code consists of indexer.py and search.py. 
Both general and field searches are possible. The search returns a ranked list of top k articles within 5 seconds. 

## Indexing:
+ Parsing: SAX Parser is used to parse the XML corpus.
+ Casefolding: Converting corpus to Lower Case.
+ Tokenisation: Removing all special characters.
+ Stop Word Removal: Stop words are removed by referring to the nltk stop word list.
+ Stemming: Using library PyStemmer.
+ Creating various intermediate index files with word to field postings.
+ Multi-way merging on the index files to create field based files along with their respective offsets.

## Searching:
+ The query is extracted from queries.txt, processed and tokenized.
+ One by one word is searched in vocabulary and the file number is noted.
+ The respective field files are opened and the document ids along with the frequencies are noted.
+ The documents are ranked on the basis of TF-IDF scores.
+ The title of the documents are extracted using title.txt

## Files Produced
+ index*.txt (intermediate files) : It consists of words with their posting list. Eg. <word> d1b2t4c5 d5b3t6l1
+ title.txt : With id-title .
+ titleOffset.txt : Offset for title.txt
+ vocab.txt : It has all the words and the file number in which those words can be found along with the document frequency.
+ offset.txt : Offset for vocab.txt
+ [b|t|i|r|l|c]*.txt : It consists of words found in various sections of the article along with document id and frequency.
+ offset_[b|t|i|r|l|c]*.txt : Offset for various field files.

## How to run:

#### python3 indexer.py <xml file dump folder> 
This function takes as input the corpus file and creates the entire index in a field separated manner. It also creates a vocabulary list and a file containg the title-id map. 
Along with these files, it also creates the offsets for all the files. 

#### python3 search.py
This function opens a shell and asks for the query to be searched. It returns the top ten results from the Wikipedia corpus.
