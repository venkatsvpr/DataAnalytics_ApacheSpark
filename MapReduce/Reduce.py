#!/usr/bin/python
import sys
word = None
prev_word = None
prev_count = 0;
for line in sys.stdin:
    line = line.strip()
    lt =  line.split('\t')
    if (len(lt)!=2):
        continue;
    word,count = lt[0],lt[1]
    count = int(count)
    if (prev_word == None):
        prev_word = word
        continue
    elif (prev_word == word):
        prev_count += count
    else:
        print(prev_word+"\t"+str(prev_count))
        prev_word = word
        prev_count = count
print (prev_word+"\t"+str(prev_count))
