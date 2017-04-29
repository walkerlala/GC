#!/usr/bin/python3
#encoding:utf-8

### 把超过100的域名去掉

#请手动改变以下设置
hostname = "localhost"
username = "root"
password = "root"
charset  = "utf8"

trainingDB_name="NEW_trainingDB"
tfidfDB_name="NEW_tfidfDB"

# output file
#trainingset_name = "training.file.new_new"

# 最终训练集输出文件
trainingset_result_name = "training_set_result"

features_number_per_tag = 100

# 如果一个域名内的网页大于这个数，就删除这个域名内的所有网页
delete_upper = 50

##################### program start ##########################
import MySQLdb
import re
import os
from collections import OrderedDict

class Xsqaure:
    """ 用卡方检验来筛选features """

    trainingDB = trainingDB_name
    tfidfDB    = tfidfDB_name

    fNR_per_tag = features_number_per_tag

    delete_threshold = delete_upper

    # 存放每个类的 features
    xinwen = []
    gouwu =[]
    shejiao = []
    youxi = []
    yinyue = []
    yuedu = []
    wangluokeji=[]
    yiliaojiangkan=[]
    shipingbofan=[]
    jiaoyuwenhua=[]
    shenghuofuwu=[]
    jiaotonglvyou=[]

    # 每个类的单词(已去重)
    all_words_xinwen = set()
    all_words_gouwu = set()
    all_words_shejiao = set()
    all_words_youxi = set()
    all_words_yinyue = set()
    all_words_yuedu = set()
    all_words_wangluokeji = set()
    all_words_yiliaojiankan = set()
    all_words_shipingbofan = set()
    all_words_jiaoyuwenhua = set()
    all_words_shenghuofuwu = set()
    all_words_jiaotonglvyou = set()

    # 一个类别对应一个号码
    tags_numbers = {
              "新闻": 1,
              "购物": 2,
              "社交": 3,
              "游戏": 4,
              "音乐": 5,
              "阅读": 6,
              "网络科技": 7,
              "医疗健康": 8,
              "视频播放": 9,
              "教育文化": 10,
              "生活服务": 11,
              "交通旅游": 12 }

    # 里面的每一个tuple都对应着数据库里面的一行
    result_tuples = []

    # 最终所有的 features
    features = []

    # tfidf 数据库的内容。先全部取出来，查询起来就会很快
    tfidf_contents = {}

    # 最终的训练集, 大概张这样
    #   1  1:0.34 2:0.44...
    #   2  1.0.22 2:0   ...
    # 第一列是他们的标签
    final_training_set_output= []

    def __init__(self):
        """ 初始化数据库连接... """

        #self.training_set_handle = open(trainingset_name, "w+")
        self.final_ts_file_handle = open(trainingset_result_name, "w+")

        self.fNR_per_tag = features_number_per_tag

        self.db = MySQLdb.connect(
                       host = hostname,
                       user = username,
                       passwd = password,
                       charset = charset)
        self.cur = self.db.cursor()

    def _cal_ABCD(self, all_tuples, word, tag):
        """ 计算卡方值的辅助函数 """
        A, B, C, D = 0, 0, 0, 0
        for t, ws in all_tuples:
            if t == tag and word in ws:
                A += 1
            elif t != tag and word in ws:
                B += 1
            elif t == tag and word not in ws:
                C += 1
            elif t != tag and word not in ws:
                D += 1
            else:
                raise Exceptioin("FATAL! logical BUG!")
        return (A, B, C, D)

    def _cal_xsquare(self, A, B, C, D):
        return pow(A * D - B*C, 2) * 22258 / ((A+C) *(A+B)*(B+D)* (C+D))

    def _do_xsquare(self, tag, tag_array, all_tuples, all_words):
        """ 计算一个类里面所有词的卡方值, 以便后面筛选 """
        #  @tag: 类别
        #  @tag_array: 存放某个词在一个类别里面的卡方值
        #  @all_tuples: 从数据库里面取出来的所有结果
        for word in all_words:
            A, B, C, D = self._cal_ABCD(all_tuples, word, tag)
            xsquare = self._cal_xsquare(A, B, C, D)
            #print("appending (%s):%f to tag_array" % (word, xsquare))
            tag_array.append((word, xsquare))

    def _filter_array(self, array):
        """ 截取卡方值最高的 self.fNR_per_tag 个词语 """
        # @array: 某个类别的array
        array = sorted(array, key = lambda tple : -tple[1])
        array = array[0 : self.fNR_per_tag]
        return array

    # @obselete
    def _output2trainingset(self):
        """ 将每个类的 features 输出到 file_handle 里面 """
        tag_num = tags_numbers[tag]
        file_handle.write(str(tag_num))
        file_handle.write(" ")
        for word, value in array:
            try:
                file_handle.write(str(value) + " ")
            except Exceptioin as e:
                print(e)
        file_handle.write("\n")

    def _output_result(self):
        """ 输出训练集 """
        for tag, dic in self.final_training_set_output:
            self.final_ts_file_handle.write(str(self.tags_numbers[tag]))
            self.final_ts_file_handle.write(" ")
            for cnt, (key, value) in enumerate(dic.items()):
                # index should start from 1, not 0
                self.final_ts_file_handle.write(str(cnt+1) + ":" + str(value) + " ")
            self.final_ts_file_handle.write("\n")

    def _get_tfidf(self, page_id, word):
        """ 从 self.tfidf_contents 中取出对应 tfidf 值 """
        for w, value in self.tfidf_contents[page_id]:
            if w == word:
                return value
        print("FATAL! no tfidf for %s found!" % w)
        return 0

    def load_training_set(self):
        """ 用卡方检验得出所有features之后，重数据库里面取出所有数据， 转换成训练集"""
        print("db name: " + str(self.trainingDB))
        self.cur.execute("SELECT count(*) from " + self.trainingDB + ".pages_table");
        for i in self.cur.fetchall():
            print("count(*) is: " + str(i))
        self.cur.execute("SELECT page_id, title, keywords, description, tag FROM "
                + self.trainingDB + ".pages_table")
        for page_id, t, k, d, tag in self.cur.fetchall():
            document = (t+k+d).strip(",").split(",")
            features_value = OrderedDict()
            for word in self.features:
                if word in document:
                    #features_value[word] = self._get_tfidf(page_id, word)
                    features_value[word] = 1
                else:
                    features_value[word] = 0
            self.final_training_set_output.append((tag, features_value))
        print("final_traiing_set_output length: %d" % len(self.final_training_set_output))

    def start_task(self):
        """ 类的对外接口. 开始执行任务 """

        """
        print("DELETE some useless pages...")
        # 先删除一些无用的
        self.cur.execute("DELETE FROM " + self.trainingDB + ".pages_table "
                + "WHERE `tag`='无类别' or tag is null or "
                + "(keywords='' and description='' and title='')")

        # 删除那些页面过多的网页
        self.cur.execute("DELETE FROM " + self.trainingDB + ".pages_table where domain_name in "
                + "(select domain_name from "
                + "(SELECT domain_name, count(domain_name) as cnt "
                + "from " + self.trainingDB + ".pages_table group by domain_name having cnt > "
                + str(self.delete_threshold) + ") as tbl)")
        print("END DELETE")

        print("Executing sql....")
        self.cur.execute("select `tag`, `title`, `keywords`, `description` from "
                + self.trainingDB + ".pages_table")
        print("END executing sql.")

        for tag, t, k, d in self.cur.fetchall():
            self.result_tuples.append((tag, (t+k+d).strip(",").split(",")))

        for tag, words in self.result_tuples:
            if tag == "新闻":
                self.all_words_xinwen.update(words)
            elif tag == "购物":
                self.all_words_gouwu.update(words)
            elif tag == "社交":
                self.all_words_shejiao.update(words)
            elif tag == "游戏":
                self.all_words_youxi.update(words)
            elif tag == "音乐":
                self.all_words_yinyue.update(words)
            elif tag == "阅读":
                self.all_words_yuedu.update(words)
            elif tag == "网络科技":
                self.all_words_wangluokeji.update(words)
            elif tag == "医疗健康":
                self.all_words_yiliaojiankan.update(words)
            elif tag == "视频播放":
                self.all_words_shipingbofan.update(words)
            elif tag == "教育文化":
                self.all_words_jiaoyuwenhua.update(words)
            elif tag == "生活服务":
                self.all_words_shenghuofuwu.update(words)
            elif tag == "交通旅游":
                self.all_words_jiaotonglvyou.update(words)
            else:
                print("FATAL !! tag NOT matching anyword: %s" % tag)

        print("Doing xsquare()...")
        self._do_xsquare("新闻", self.xinwen, self.result_tuples, self.all_words_xinwen)
        self._do_xsquare("购物", self.gouwu, self.result_tuples, self.all_words_gouwu)
        self._do_xsquare("社交", self.shejiao, self.result_tuples, self.all_words_shejiao)
        self._do_xsquare("游戏", self.youxi, self.result_tuples, self.all_words_youxi)
        self._do_xsquare("音乐", self.yinyue, self.result_tuples, self.all_words_yinyue)
        self._do_xsquare("阅读", self.yuedu, self.result_tuples, self.all_words_yuedu)
        self._do_xsquare("网络科技", self.wangluokeji, self.result_tuples, self.all_words_wangluokeji)
        self._do_xsquare("医疗健康", self.yiliaojiangkan, self.result_tuples, self.all_words_yiliaojiankan)
        self._do_xsquare("视频播放", self.shipingbofan, self.result_tuples, self.all_words_shipingbofan)
        self._do_xsquare("教育文化", self.jiaoyuwenhua, self.result_tuples, self.all_words_jiaoyuwenhua)
        self._do_xsquare("生活服务", self.shenghuofuwu, self.result_tuples, self.all_words_shenghuofuwu)
        self._do_xsquare("交通旅游", self.jiaotonglvyou, self.result_tuples, self.all_words_jiaotonglvyou)
        print("END xsquare().")

        print("Filtering array...")
        self.xinwen = self._filter_array(self.xinwen)
        self.gouwu = self._filter_array(self.gouwu)
        self.shejiao = self._filter_array(self.shejiao)
        self.youxi = self._filter_array(self.youxi)
        self.yinyue = self._filter_array(self.yinyue)
        self.yuedu = self._filter_array(self.yuedu)
        self.wangluokeji = self._filter_array(self.wangluokeji)
        self.yiliaojiangkan = self._filter_array(self.yiliaojiangkan)
        self.shipingbofan = self._filter_array(self.shipingbofan)
        self.jiaoyuwenhua = self._filter_array(self.jiaoyuwenhua)
        self.shenghuofuwu = self._filter_array(self.shenghuofuwu)
        self.jiaotonglvyou = self._filter_array(self.jiaotonglvyou)
        print("END filtering array.")

        for word, value in self.xinwen:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.gouwu:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.shejiao:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.youxi:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.yinyue:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.yuedu:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.wangluokeji:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.yiliaojiangkan:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.shipingbofan:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.jiaoyuwenhua:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.shenghuofuwu:
            self.features.append(word) if word else print("Empty string in features")
        for word, value in self.jiaotonglvyou:
            self.features.append(word) if word else print("Empty string in features")

        print("Quering tfidf DB...")
        self.cur.execute("SELECT page_id, word_value, tf_idf FROM "
                + self.tfidfDB + ".tfidf_training")
        for pid, word, tfidf in self.cur.fetchall():
            if pid in self.tfidf_contents.keys():
                self.tfidf_contents[pid].append((word, tfidf))
            else:
                self.tfidf_contents[pid] = [(word, tfidf)]
        print("END quering tfidf DB.")
        """

        # load features
        features_handle = open("features","r")
        fs = set()
        for line in features_handle:
            fs.add(line.strip())
        self.features.extend(fs)

        self.load_training_set()

        print("Outputing file...")
        self._output_result()

        """
        print("Outputing file...")
        self._output2trainingset(self.xinwen, self.training_set_handle, "新闻", self.tags_numbers)
        self._output2trainingset(self.gouwu, self.training_set_handle, "购物", self.tags_numbers)
        self._output2trainingset(self.shejiao, self.training_set_handle, "社交", self.tags_numbers)
        self._output2trainingset(self.youxi, self.training_set_handle, "游戏", self.tags_numbers)
        self._output2trainingset(self.yinyue, self.training_set_handle, "音乐", self.tags_numbers)
        self._output2trainingset(self.yuedu, self.training_set_handle, "阅读", self.tags_numbers)
        self._output2trainingset(self.wangluokeji, self.training_set_handle, "网络科技", self.tags_numbers)
        self._output2trainingset(self.yiliaojiangkan, self.training_set_handle, "医疗健康", self.tags_numbers)
        self._output2trainingset(self.shipingbofan, self.training_set_handle, "视频播放", self.tags_numbers)
        self._output2trainingset(self.jiaoyuwenhua, self.training_set_handle, "教育文化", self.tags_numbers)
        self._output2trainingset(self.shenghuofuwu, self.training_set_handle, "生活服务", self.tags_numbers)
        self._output2trainingset(self.jiaotonglvyou, self.training_set_handle, "交通旅游", self.tags_numbers)
        print("END outputing file...")
        """

if __name__ == "__main__":
    pre = Xsqaure()
    pre.start_task()
