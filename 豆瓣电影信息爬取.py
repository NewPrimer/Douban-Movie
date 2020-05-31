import os
import re
import time
import pandas
import pickle
import sqlite3
import requests
from math      import log
from lxml      import etree
from copy      import deepcopy
from functools import reduce

def getInteger(s):
    return int(re.findall(r'\d+',s)[0])

def getChinese(s):
    return re.findall(r'[\u4e00-\u9fa5|·]+',s)

def SaveToExcel(total):
    ff=pandas.DataFrame(total)
    ff.to_excel('豆瓣热门电影2.xls')

def SaveToSQL3(datas):
    flag=os.path.exists("./豆瓣热门电影2.db") #数据库文件存在标记
    
    #连接到数据库文件
    conn=sqlite3.connect("豆瓣热门电影2.db")
    if flag==False:
        conn.execute('''CREATE TABLE CNAME
           (TITLE         TEXT,
            CASTS         TEXT,
            DIRECTORS     TEXT,
            GENRES        TEXT,
            COUNTRY       TEXT,
            LANG          TEXT,
            YEAR          INT,
            DATE          TEXT,
            TIME          INT,
            STAR          FLOAT,
            RATING_PEOPLE INT,
            INTRODUCTION  TEXT,
            URL           TEXT
           );''')
    
    #将列表写入数据库
    for i in datas:
        #注意sql语句中使用了格式化输出的占位符%s和%d来表示将要插入的变量，其中%s需要加引号''
        sql="insert into cname(title,casts,directors,genres,country,lang,"\
            "year,date,time,star,rating_people,introduction,url)values"\
            "('%s','%s','%s','%s','%s','%s',%d,'%s',%d,%f,%d,'%s','%s')"\
            %(i['电影名称'],i['主演'],i['导演'],i['影片类型'],i['制片国家/地区'],
              i['语言'],i['年份'],i['上映日期'],i['片长'],i['豆瓣评分'],
              i['评论人数'],i['简介'],i['链接'])
        conn.execute(sql)
        conn.commit()
    
    #关闭数据库连接
    conn.close()

def getHTMLtext(url):
    try:
        r=requests.get(url,headers=header,proxies=proxies)
        r.raise_for_status()
        return r.json()
        # 返回json，在浏览器审查，network当中的xrh里面可以检测到，用到了JSONView浏览器插件
    except:
        return print('异常')

def getMovieInfo(name,url):
    def getInfo(s):
        tmp=selector.xpath(s)
        return len(tmp)==0 and "NotDefined" or reduce(strCat,tmp)
    
    html=requests.get(url,headers=header,proxies=proxies).content.decode("utf-8")
    selector=etree.HTML(html)
    strCat=lambda x,y:x+'/'+y
    
    #年份
    year=getInfo("//h1/span[@class='year']/text()")
    #导演
    director=getInfo("//a[@rel='v:directedBy']/text()")
    #上映日期
    date=getInfo("//span[@property='v:initialReleaseDate']/text()")
    #时长
    time=getInfo("//span[@property='v:runtime']/text()")
    #豆瓣评分
    star=getInfo("//strong[@class='ll rating_num']/text()")
    #评论人数
    rating_people=getInfo("//span[@property='v:votes']/text()")
    #影片类型
    genres=getInfo("//span[@property='v:genre']/text()")
    #制片国家/地区
    country=re.findall(r'制片国家/地区:</span>(.*?)<br/>',html)
    #语言
    lang=re.findall(r'语言:</span>(.*?)<br/>',html)
    #简介
    introduction=getInfo("//span[@property='v:summary']/text()")
    
    #转化成合适的类型
    star=float(star)
    director=str(director)
    rating_people=int(rating_people)
    time=getInteger(time)
    year=getInteger(year)
    country=(country[0].split())[0]
    lang=(lang[0].split())[0]
    introduction=(introduction.split())[0]
    if introduction=='NotDefined':
        introduction='无'
    
    #将各个元素集合成字典
    movie_info={"导演":director,
                "影片类型":genres,
                "制片国家/地区":country,
                "语言":lang,
                "年份":year,
                "上映日期":date,
                "片长":time,
                "豆瓣评分":star,
                "评论人数":rating_people,
                '简介':introduction,
                '链接':url,
                }
    return movie_info

def parsehtml(html):
    total=[]
    for i in range(5):
        # 定位到每个电影下，并返回一个字典
        file=html['data'][i]
        title=file['title']
        casts=' '.join(file['casts'])
        url=file['url']
        datas=getMovieInfo(title,url)
        base={'电影名称':title,'主演':casts}
        data={}
        data.update(base)
        data.update(datas)
        total.append(data)
        print('正在爬取的电影：%s'%title)
    return total

def DataAnalysis(datas):
    countries=[]
    directors=[]
    movieType=[]
    #对各电影按豆瓣评分排序
    datas.sort(key=lambda x:x['豆瓣评分'],reverse=True)
    for i in datas:
        if (i['制片国家/地区'] in countries)==False:
            countries.append(i['制片国家/地区'])
        
        #导演可能有多个，故逐个提取
        types=getChinese(i['导演'])
        for j in types:
            if (j in directors)==False:
                directors.append(j)
        
        #影片类型可能有多个，故逐个提取
        types=getChinese(i['影片类型'])
        for j in types:
            if (j in movieType)==False:
                movieType.append(j)
    
    #复制countries，为后面的各国电影发展分析做准备
    t=deepcopy(movieType)
    t=list(map(lambda x:[x,0],t))
    d=deepcopy(directors)
    d=list(map(lambda x:[x,0],d))
    cou=deepcopy(countries)
    cou=list(map(lambda x:[x,0],cou))
    
    #统计各个国家和导演的平均电影评分
    type_8=deepcopy(t)
    director_8=deepcopy(d)
    country_8=deepcopy(cou)
    countries=list(map(lambda x:[x,0,0,0],countries))
    directors=list(map(lambda x:[x,0,0,0,0,0],directors))
    def acc(src,dest):
        for j in getChinese(src):
            for k in dest:
                if k[0]==j:
                    k[1]+=1
    for i in datas:
        if i['豆瓣评分']>=8:
            for j in country_8:
                if j[0]==i['制片国家/地区']:
                    j[1]+=1
            acc(i['影片类型'],type_8)
            acc(i['导演'],director_8)
        for j in countries:
            if j[0]==i['制片国家/地区']:
                j[1]+=i['豆瓣评分']
                j[2]+=1
                break
        for j in getChinese(i['导演']):
            for k in directors:
                if k[0]==j:
                    k[1]+=i['豆瓣评分']
                    k[2]+=1
                    k[4]+=i['评论人数']
                    break
    
    #为各个国家和导演计算平均评分，保留两位小数
    for i in range(len(countries)):
        countries[i][3]=round(countries[i][1]/countries[i][2],2)
    for i in range(len(directors)):
        directors[i][3]=round(directors[i][1]/directors[i][2],2)
    
    #评价各位导演的影响力
    max_star=max(list(map(lambda x:x[3],directors)))
    max_movie=max(list(map(lambda x:x[2],directors)))
    max_rating_people=max(list(map(lambda x:x[4],directors)))
    for i in directors:
        a=0.3*log(i[3]+1)/log(max_star+1)
        b=0.4*log(i[2]+1)/log(max_movie+1)
        c=0.3*log(i[4]+1)/log(max_rating_people+1)
        i[5]=round(100*(a+b+c),2)
    directors.sort(key=lambda x:x[5],reverse=True) #对各位导演按影响力进行排序
    
    #分析各个年代各国、各地区电影数量发展趋势
    years=list(map(lambda x:[x['制片国家/地区'],x['年份']],datas)) #建立数据表
    years.sort(key=lambda x:x[1]) #按年代从远到近排序
    years_min=int(years[0][1]/10)*10 #最久远的年代
    
    #各个年代各国电影数量统计
    a_years=deepcopy(cou)
    all_years=[]
    for i in years:
        if i[1]>=years_min+10:
            all_years.append([years_min,dict(a_years)])
            years_min+=10
            a_years=deepcopy(cou)
        for j in range(len(a_years)):
            if a_years[j][0]==i[0]:
                a_years[j][1]+=1
    all_years.append([years_min,dict(a_years)])
    
    #对豆瓣评分大于8分的电影数量最多的国家和类型按从高到低排序
    type_8.sort(key=lambda x:x[1],reverse=True)
    country_8.sort(key=lambda x:x[1],reverse=True)
    
    #将列表转化为字典
    type_8=dict(type_8)
    all_years=dict(all_years)
    country_8=dict(country_8)
    director_8=dict(director_8)
    
    print(countries)
    print(directors)
    print(all_years)
    print(country_8)
    print(director_8)
    print(type_8)

if __name__=='__main__':
    filename="total.dat"
    if os.path.exists("./total.dat")==False:
        total=[]
        proxies={'https':'https://127.0.0.1:1080',
                 'http':'http://127.0.0.1:1080'}
        header={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 '
                'Safari/537.36','Referer':'https://movie.douban.com/tag/',
                'Host':'movie.douban.com'
                }
        for i in range(1):
            url="https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=电影&start={}&genres=".format(i*20)
            #热门电影爬取
            total+=parsehtml(getHTMLtext(url))
            time.sleep(5)
        f=open(filename,'wb')
        pickle.dump(total,f)
        f.close()
    else:
        f=open(filename,'rb')
        total=pickle.load(f)
    #SaveToExcel(total)
    #SaveToSQL3(total)
    DataAnalysis(total)