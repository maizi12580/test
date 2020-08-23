#!/usr/bin/python
# -*- coding: UTF-8 -*-
#@author maizi
#2019/10/12

class Person:
    personCount = 0
    id = 0
    def __init__(self, id):
        self.id = id
    def getId(self):
        print self.id
    def Count(self):
        print "Total" % Person.personCount
    def _del_(self):
        class_name = self.__class__.__name__
        print class_name,"此类暂无使用，被销毁" #当所有的引用被删除时，这个类会被回收销毁

person1 = Person(12580)
person1.getId()

class Women(Person):
    WomenCount = 0
    def __init__(self,name):
        self.name = name
    def getName(self):
        print self.name
women = Women('maizi')
women.getName()
women.getId()

