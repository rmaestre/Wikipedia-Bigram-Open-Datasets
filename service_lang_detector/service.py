#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
    Api rest

"""
import cherrypy
import getopt
import sys
import yaml
from kyotocabinet import DB
import time
import json



class Go(object):
                
    def flatten(self , text):
        """
        """
        text = text.encode('utf-8')
        text = text.lower()
        text = text.replace('\t' , '')
        text = text.replace('\r\n' , '')
        text = text.replace('\r' , '')
        text = text.replace('\n' , '')
        text = text.replace('“' , '')
        text = text.replace('"' , '')
        text = text.replace('(' , '')
        text = text.replace(')' , '')
        text = text.replace('!' , '')
        text = text.replace('¡' , '')
        text = text.replace('?' , '')
        text = text.replace('¿' , '')
        text_mark = text.replace(',' , '')
        text_mark = text_mark.replace(';' , '')
        text_mark = text_mark.replace(':' , '')
        return text_mark
        
    def __init__(self):
        """
        """
        self.dbs = {}
        
        # Open ES database
        self.dbs['es'] = {}
        db_es = DB()
        db_es.open('data/es_ngrams.kch' , DB.OREADER)
        self.dbs['es']['db'] = db_es
        self.dbs['es']['max'] = 4292221
        self.dbs['es']['score'] = 0
        
        # Open FR database
        self.dbs['fr'] = {}
        db_fr = DB()
        db_fr.open('data/fr_ngrams.kch' , DB.OREADER)
        self.dbs['fr']['db'] = db_fr
        self.dbs['fr']['max'] = 6682359
        self.dbs['fr']['score'] = 0
        
        # Open CA database
        self.dbs['ca'] = {}
        db_ca = DB()
        db_ca.open('data/ca_ngrams.kch' , DB.OREADER)
        self.dbs['ca']['db'] = db_ca
        self.dbs['ca']['max'] = 1545446
        self.dbs['ca']['score'] = 0
        
        # Open EN database
        self.dbs['en'] = {}
        db_en = DB()
        db_en.open('data/en_ngrams.kch' , DB.OREADER)
        self.dbs['en']['db'] = db_en
        self.dbs['en']['max'] = 2393467
        self.dbs['en']['score'] = 0
        
        
    def index(self , text):
        """
        """
        # Process text
        text = self.flatten(text)
        words = text.lower()
        words_chunks = words.split(' ')
        # Res
        res = {}
        # First remove intersection words
        res['debug'] = {}
        t_start = time.time()
        for db in self.dbs:
            res['debug'][db] = []
            n = len(words_chunks)
            c = 0
            matchs_number = 0
            quartiles = []
            while c < n:
                word_aux = ' '.join(words_chunks[c:c+2])
                score = self.dbs[db]['db'].get(word_aux)
                if score != None:
                    score_aux = (float(score)*100)/float(self.dbs[db]['max'])
                    quartiles.append(score_aux)
                    res['debug'][db].append('%s,%d,%.4f' % (word_aux, float(score), float(score_aux)))
                    matchs_number += 1
                c += 1
            self.dbs[db]['quartiles'] = quartiles
            self.dbs[db]['matchs'] = matchs_number
        for db in self.dbs:
            res[db] = {
                        'matchs' : self.dbs[db]['matchs'],
                        'sum_quartiles': sum(self.dbs[db]['quartiles'])}
        # Return json repsonse
        res['time'] =  time.time() - t_start
        return json.dumps(res)
    index.exposed = True
        
        
def main(argv):
    
    # Command line arguments
    try:
        optlist , args = getopt.getopt(argv , 'p:b:' , ['help'])
        for opt, value in optlist:
            if opt == '--help':
                usage()
                sys.exit()
            elif opt == "-p":
                port = int(value)
            elif opt == "-b":
                bind_address = value
            elif opt == "-f":
                conf_file = value
    except getopt.GetoptError , err:
        print '\n' + str(err)
        usage()
        sys.exit(2)
    cherrypy.quickstart(Go())


if __name__ == '__main__':
    main(sys.argv[1:])
