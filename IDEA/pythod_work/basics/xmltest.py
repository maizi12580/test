#!/usr/bin/python
# -*- coding: UTF-8 -*-
# @author maizi
# 2019/10/14

import xml.sax

class Handler(xml.sax.ContentHandler):
    def __init__(self):
        self.CurrentData = ""
        self.type = ""
        self.format = ""
        self.year = ""
        self.rating = ""
        self.stars = ""
        self.description = ""
    #元素开始事件处理
    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if tag == "movie":
            title = attributes["title"]
            print "电影名", title

    #元素结束事件处理
    def endElement(self, tag):
        if self.CurrentData == "type": print "类型", self.type
        if self.CurrentData == "year": print "年份", self.year
        if self.CurrentData == "stars": print "座位", self.stars
        if self.CurrentData == "description": print "细节", self.description

    #内容事件处理
    def characters(self, content):
        if self.CurrentData == "type": self.type = content
        elif self.CurrentData == "year": self.year = content
        elif self.CurrentData == "stars": self.stars = content
        elif self.CurrentData == "description": self.description = content

if(__name__ == "__main__"):
    # 创建一个 XMLReader
    parser = xml.sax.make_parser()
    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)

    # 重写 ContextHandler
    Handler = Handler()
    parser.setContentHandler( Handler )
    parser.parse("1.xml")

