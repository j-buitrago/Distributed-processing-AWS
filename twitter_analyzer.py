import json
import csv
from mrjob.job import MRJob
from mrjob.step import MRStep

#Esta función recibe el path del fichero AFINN-111.txt y lo lee guargando en un diccionario las claves y valores. 
def read_word_file(filename):

    word_dict = {}
    with open(filename, encoding='UTF-8') as fid:
        for n, line in enumerate(fid):
            try:
                #separamos por tabulaciones y limpiamos carácteres extraños.
                word, score = line.strip().split('\t')
            except ValueError:
                msg = 'Error in line %d of %s' % (n + 1, filename)
            word_dict[word] = int(score)
    return word_dict
#Esta función recibe el path del fichero States-USA.csv, diccionario que nos servirá para comprobar si el estado que aparece en el tweet es correcto.
def read_states_file(filename):
    states = dict()
    with open(filename, 'r') as csvfile:
        file = csv.reader(csvfile)
        for line in file:
            states.setdefault(line[1],line[0])
    return states
#Esta función devuelve para el texto que recibe un score calculado como la suma de las puntuaciones de las palabras en el diccionario AFINN-111.txt.
def get_score(text):

    words = text.split()
    score = 0
    for word in words:
        score += float(word_dict.get(word.lower(), 0.0))
    return score
#Esta función recibe un tweet en formato .json, filtrando los tweets que queremos usar y devuelve el estado y el texto del tweet.
def clean_data(line):
    
    data=json.loads(line)
    tweetText=''
    state=None
    #Solo nos quedamos con las trazas que son tweets y que tienen información en el campo place.
    if "created_at" in data and data['place']!=None:
        #Filtramos y cogemos solo los de estados unidos y comprobamos que la ubicación iba separada por ",".
        if data['place']['country_code'] == 'US' and len(data['place']["full_name"].split(','))>1:
            #Comprobamos si es tweet es "extended" o normal, puesto que la información va en un campo diferente.
            if 'extended_tweet' in data:
                tweetText = data['extended_tweet']['full_text']
            else:
                tweetText = data['text']

            loc = (data['place']["full_name"])
            #Adquirimos la informacion del estado y comprobamos que este en el diccionario States-USA.csv cargado en la función read_states_file
            try:
                c = loc.split(',')
                codigo = c[len(c)-1].strip()
                state = loc.split(',')[1].strip().lower()
                if state == 'usa':
                    state = loc.split(',')[0].strip().lower()
            except:
                c = loc.split(' ')
                codigo = c[len(c)-1].strip()
                state = loc.split(' ')[1].strip().lower()
                if state == 'usa':
                    state = loc.split(' ')[0].strip().lower()
            
            if codigo in states:
                state = states[codigo]              
    return state, tweetText

class twitter_analyzer(MRJob):

    def configure_options(self):
      super(twitter_analyzer, self).configure_options()
      #Permitimos dos argumentos de entrada que serán los dos diccionarios necesarios para ejecutar el programa (AFINN-111.txt y States-USA.csv)
      self.add_file_arg('--states')
      self.add_file_arg('--dic')

    def steps(self):
      return[
         MRStep(mapper=self.mapper, reducer=self.reducer)
      ]
    #La función mapper recibe el tweet en bruto y aplica la función clean_data, obtiene el score para ese tweet
    #y añade al diccionario de salida el score obtenido para el estado correspondiente.
    def mapper(self, _, line):
        state, tweet = clean_data(line)
        if state is not None and state in states.values():
            score = get_score(tweet)
            #Si el estado ya existia sumamos la nueva puntuación e incrementamos 1 el contador.
            if state in output:
                output[state][0] += score
                output[state][1] += 1
            #Si no existe todavía creamos el nuevo estado con su puntuación obtenida.    
            else:
                info = [score, 1]
                output.setdefault(state, info)
            info = output[state]
            #Por ejemplo:"north carolina, [4,10]"     
            yield(state, info)                                  
    #La función reducer recibe la salida antes comentada del mapper y con esos datos calcula la media del estado. 
    def reducer(self, state, info):
       values=0
       for p in info:
           values=p        
       try:
           media = round(values[0]/values[1], 2)
           yield(state, media)
       except:
           pass

  
if __name__ == '__main__':
    
    word_dict = read_word_file('AFINN-111.txt')
    states = read_states_file('States-USA.csv')
    #Este diccionario lo utilizaremos para almacenar para cada estado su info. Info contiene el score total
    #para ese estado y el contador de tweets de ese estado para poder hacer la media
    output = dict()
    
    twitter_analyzer.run()