from Pyro4 import expose
import re
import time

from functools import wraps
import traceback


def wrapper(method):
    @wraps(method)
    def wrapped(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except:
            e = traceback.format_exc().replace('\n', ' ')
            raise Exception(e)

    return wrapped


class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers
        print("Inited")

    @wrapper
    def solve(self):
        print("Job Started")
        print("Workers %d" % len(self.workers))
        d_start = time.time()
        self.dic = self.Dictionary(self.input_file_name)
        d_end = time.time()

        file = open(self.input_file_name, 'r')
        word = "----------"
        line = ""
        while word not in line:
            line = file.readline()

        all_words = []

        while line != '':
            line = file.readline()
            words = line.split()
            all_words.append(words)

        all_words = [self.dic.clean_data(word) for line in all_words for word in line]

        # map
        mapped = []
        workers_count = len(self.workers)
        start = time.time()
        l = list(self.dic.dictionary)


        for i in xrange(0, workers_count):
            chunk = (all_words[int(i / workers_count * len(all_words)):int((i + 1) / workers_count * len(all_words))])
            mapped.append(self.workers[i].mymap(l, chunk))



        # reduce
        reduced = self.myreduce(mapped)
        end = time.time()
        all_words_count = len(all_words)
        self.write_output("checked words: " + str(reduced))
        self.write_output("all words: " + str(all_words_count))
        self.write_output("Elapsed time for Dictionary creation: " + str(d_end - d_start))
        self.write_output("Predicting accuracy: " + str(reduced * 1.0 / all_words_count))
        self.write_output("Elapsed time for predicting: " + str(end - start))
        print("Job Finished")

    @staticmethod
    @expose
    def mymap(dictionary, words):
        start = time.time()
        res = 0
        if len(words) != 0:
            words = [item for sublist in words for item in sublist]
        for word in words:
            if word in dictionary:
                res += 1
        end = time.time()
        return res

    @staticmethod
    @expose
    def myreduce(mapped):
        output = 0
        for x in mapped:
            # tempo = x.value
            # value = [item for sublist in tempo for item in sublist]

            output += x.value
        return output

    def write_output(self, output):
        f = open(self.output_file_name, 'a')
        f.write(str(output))
        f.write('\n')
        f.close()

    class Dictionary:
        def __init__(self, input_file_name):
            self.dictionary = {}
            with open(input_file_name, 'r') as file:
                for line in file:
                    if "----------" in line:
                        break
                    word = self.clean_data(line)
                    self.dictionary[word] = word

        def check(self, word):
            return self.dictionary.get(word)

        @staticmethod
        def clean_data(message):
            clean_message = message.lower()
            clean_message = re.sub(r'(\d+)[^ 0-9a-z]+(\d+)', r'\1\2', clean_message)
            clean_message = re.sub(r'[^a-z0-9]', ' ', clean_message)
            clean_message = re.sub(r' +', ' ', clean_message)
            clean_message = clean_message.strip()

            return clean_message
