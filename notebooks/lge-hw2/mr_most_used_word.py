
from mrjob.job import MRJob
from sys import stderr
import re

WORD_RE = re.compile(r'[\w\']+')
# WORD_RE = re.compile(r"^Fri [A-Z][a-z][a-z] [0-9 ][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9].[0-9][0-9][0-9].*$")

class MRMostUsedWord(MRJob):
  def mapper_get_words(self, _, line):
    for word in WORD_RE.findall(line):
      yield word, 1

  def combiner_count_words(self, key, values):
    val_list = list(values)
    stderr.write('combiner: ' + key + ': ' + str(val_list) + '\n')
    yield key, sum(val_list)

  def reducer_count_words(self, key, values):
    val_list = list(values)
    stderr.write('reducer: ' + key + ': ' + str(val_list) + '\n')
    yield None, (sum(val_list), key)

  def reducer_find_most_freq_word(self, _, values):
    occurrence, most_freq_word = max(values)
    yield most_freq_word, occurrence

  def steps(self):
    return [
      self.mr(mapper = self.mapper_get_words,
              combiner = self.combiner_count_words,
              reducer = self.reducer_count_words),
      self.mr(reducer = self.reducer_find_most_freq_word)
    ]

if __name__ == '__main__':
  MRMostUsedWord.run()
