#_*_coding:utf-8 _*_

from snownlp import SnowNLP


# 对snownlp的各个功能进行封装
class SentimentAnalyse:

    def __init__(self, comment):
        self.s = SnowNLP(comment)

    def get_comment_label(self):
        pass

    def get_sentiment_score(self):
        return self.s.sentiments

    def get_sentences(self):
        return self.s.sentences

    def get_words(self):
        return self.s.words