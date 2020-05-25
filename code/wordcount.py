# -*- coding: utf-8 -*-
#Frequent Words in a text
import collections
import pandas as pd
def WordCount(entityDescription):
    #entityDescription = "Brazil Listen/brəˈzɪl/, officially the Federative Republic of Brazil, is the largest country in South America. It is the world's fifth largest country, both by geographical area and by population with over 192 million people. It is the only Portuguese-speaking country in the Americas and the largest lusophone country in the world. Bounded by the Atlantic Ocean on the east, Brazil has a coastline of 7,491 km (4,655 mi)."
    a = entityDescription
    # Stopwords
    stopwords = set(line.strip() for line in open('stopwords.txt'))
    stopwords = stopwords.union(set(['mr','mrs','one','two','said','by','a','with','as','the','of','is','was','for','and','in']))
    # Instantiate a dictionary, and for every word in the file, 
    # Add to the dictionary if it doesn't exist. If it does, increase the count.
    wordcount = {}
    # To eliminate duplicates, remember to split by punctuation, and use case demiliters.
    for word in a.lower().split():
        word = word.replace(".","")
        word = word.replace(",","")
        word = word.replace(":","")
        word = word.replace("\"","")
        word = word.replace("!","")
        word = word.replace("â€œ","")
        word = word.replace("â€˜","")
        word = word.replace("*","")
        if word not in stopwords:
            if word not in wordcount:
                wordcount[word] = 1
            else:
                wordcount[word] += 1
    # Print most common word
    finalRes=[]
    n_print = 10 #number of most common words
    #print("\n. The {} most common word(s) is/are as follows\n".format(n_print))
    word_counter = collections.Counter(wordcount)
    for word, count in word_counter.most_common(n_print):
        #print(word, ": ", count)
        finalRes.append(word)

    return(finalRes)

