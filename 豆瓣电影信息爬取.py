import os
import re
import pandas
import pickle
import sqlite3
import requests
from math      import log
from lxml      import etree
from copy      import deepcopy
from functools import reduce
from datetime  import datetime

def getInteger(s):
    return int(re.findall(r'\d+',s)[0])

def getChinese(s):
    return re.findall(r'[\u4e00-\u9fa5|·]+',s)

def getCookie():
    f=open("cookie.txt","r")
    string=f.read()
    f.close()
    return string

def SaveValue(value,FileName):
    f=open(FileName,'wb')
    pickle.dump(value,f)
    f.close()

def LoadValue(FileName):
    f=open(FileName,'rb')
    value=pickle.load(f)
    f.close()
    return value

def spider():
    limit=370 #limit乘以20即为要爬取的电影数量
    fn_i='i.dat'
    fn_err='err.dat'
    filename='information.dat'
    
    #由于可能存在上一次爬取且中途失败，则加载之前的数据并继续爬取
    try:
        information=LoadValue(filename)
    except:
        information=[]
    
    #爬取到的电影数量未达到要求或为0，则继续爬取
    if len(information)<limit*20:
        import time
        
        #若存在上一次爬取，则加载上一次爬取到的位置，若为首次爬取则从头开始
        try:
            i=LoadValue(fn_i)
        except:
            i=0
        try:
            err=LoadValue(fn_err)
        except:
            err=0
        
        #爬取热门电影
        print('爬取中，请耐心等待......')
        limit+=err
        while i<limit:
            url="https://movie.douban.com/j/new_search_subjects?sort=U&range=0,10&tags=电影&start={}&genres=".format(i*20)
            temp=parsehtml(getHTMLtext(url),i-err) #热门电影爬取
            if temp==None:
                print('爬取过程中因出现意外而中断')
                break
            elif temp==[]:
                err+=1
                limit+=1
            else:
                information+=temp
            i+=1
            time.sleep(5)
        
        SaveValue(i,fn_i) #记录爬取进度
        SaveValue(err,fn_err) #记录错误次数
        SaveValue(information,filename) #将存储电影信息的变量保存至文件中
        time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        SaveValue(time,'time.dat') #记录爬取时间
    
    return information

def SaveToExcel(total):
    print('正在保存至Excel表格文件......')
    ff=pandas.DataFrame(total)
    ff.to_excel('豆瓣热门电影.xls')

def SaveToSQL3(datas):
    print('正在保存至SQLite3数据库文件......')
    
    #数据库文件存在标记
    flag=os.path.exists("./豆瓣热门电影.db")
    
    #连接到数据库文件
    conn=sqlite3.connect("豆瓣热门电影.db")
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
        r=requests.get(url,headers=header)
        r.raise_for_status()
        return r.json()
        #返回json，在浏览器审查，network当中的xrh里面可以检测到，用到了JSONView浏览器插件
    except:
        return print('爬取过程中出现异常')
#,proxies=proxies
def getMovieInfo(name,url):
    def getInfo(s):
        tmp=selector.xpath(s)
        return len(tmp)==0 and "NotDefined" or reduce(strCat,tmp)
    
    html=requests.get(url,headers=header).content.decode("utf-8")
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
    try:
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
    except:
        return None #读取到的数据有误，则直接丢弃
    
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

def parsehtml(html,num):
    total=[]
    for i in range(20):
        #定位到每个电影下，并返回一个字典，若网页返回信息异常则返回错误
        try:
            file=html['data'][i]
            title=file['title']
            casts=' '.join(file['casts'])
            url=file['url']
            datas=getMovieInfo(title,url)
            
            #返回的数据异常，则直接放弃该条数据
            if datas==None:
                return []
        except:
            print(html)
            return None
        
        base={'电影名称':title,'主演':casts}
        data={}
        data.update(base)
        data.update(datas)
        total.append(data)
        print('已爬取%d部电影'%(num*20+i+1))
    return total

def DataAnalysis(datas):
    countries=[]
    directors=[]
    movieType=[]
    
    #对各电影按豆瓣评分排序
    #datas.sort(key=lambda x:x['豆瓣评分'],reverse=True)
    
    #提取所有的国家、导演和影片类型
    for i in datas:
        def collect(data,word):
            types=getChinese(word)
            for j in types:
                if (j in data)==False:
                    data.append(j)
        
        collect(countries,i['制片国家/地区'])
        collect(directors,i['导演'])
        collect(movieType,i['影片类型'])
    
    #对各元素进行拓展
    t=list(map(lambda x:[x,0],movieType))
    d=list(map(lambda x:[x,0],directors))
    cou=list(map(lambda x:[x,0],countries))
    y=list(map(lambda x:[x,0,0,0,0],countries))
    
    moiveRank=list(map(lambda x:[x['电影名称'],x['豆瓣评分']],datas))
    moiveRank.sort(key=lambda x:x[1],reverse=True)
    movie_top100=dict(moiveRank[:100])
    movie_bottom100=dict(moiveRank[-1:-101:-1])
    
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
        for j in getChinese(i['制片国家/地区']):
            for k in countries:
                if k[0]==j:
                    k[1]+=i['豆瓣评分']
                    k[2]+=1
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
    years=list(map(lambda x:[x['制片国家/地区'],x['年份'],x['豆瓣评分'],x['评论人数']],datas)) #建立数据表
    years.sort(key=lambda x:x[1]) #按年代从远到近排序
    years_min=int(years[0][1]/10)*10 #最久远的年代
    
    #各个年代各国电影数量统计
    a_years=deepcopy(y)
    all_years=[]
    def avg_star():
        #计算各国在该年代的电影平均分
        for j in range(len(a_years)):
            if a_years[j][1]!=0:
                a_years[j][2]=round(a_years[j][2]/a_years[j][1],2)
        
        #计算该年代各国电影影响力评分
        max_star=max(list(map(lambda x:x[2],a_years)))
        max_movie=max(list(map(lambda x:x[1],a_years)))
        max_rating_people=max(list(map(lambda x:x[3],a_years)))
        for j in a_years:
            a=0.3*log(j[2]+1)/log(max_star+1)
            b=0.4*log(j[1]+1)/log(max_movie+1)
            c=0.3*log(j[3]+1)/log(max_rating_people+1)
            j[4]=round(100*(a+b+c),2)
        a_years.sort(key=lambda x:x[4],reverse=True)
        all_years.append([years_min,a_years])
    
    #按年代划分
    for i in years:
        if i[1]>=years_min+10:
            avg_star()
            years_min+=10
            a_years=deepcopy(y)
        for j in range(len(a_years)):
            if a_years[j][0]==i[0]:
                a_years[j][1]+=1
                a_years[j][2]+=i[2]
                a_years[j][3]+=i[3]
    avg_star()
    
    #提取各个年代各国电影影响力排行
    years_sort=deepcopy(all_years)
    for i in years_sort:
        i[1]=dict(list(map(lambda x:[x[0],x[4]],i[1])))
    
    #对豆瓣评分大于8分的电影数量最多的国家和类型按从高到低排序
    type_8.sort(key=lambda x:x[1],reverse=True)
    country_8.sort(key=lambda x:x[1],reverse=True)
    
    type_8=list(map(lambda x:[x[0],x[1],0],type_8))
    country_8=list(map(lambda x:[x[0],x[1],0],country_8))
    len_type_8=len(type_8)
    len_country_8=len(country_8)
    for i in type_8:
        i[2]=i[1]/len_type_8
    for i in country_8:
        i[2]=i[1]/len_country_8
    
    #将列表转化为字典
    #type_8=dict(type_8)
    #country_8=dict(country_8)
    director_8=dict(director_8)
    all_years=dict(all_years)
    years_sort=dict(years_sort)
    
    data_assemblage={'评分超过8分的电影的主要类型':type_8,
                     '评分超过8分的电影的国家分布':country_8,
                     '评分超过8分的电影的导演分布':director_8,
                     '各年代各国电影发展状况':all_years,
                     '各年代各国电影影响力排行':years_sort,
                     '评分最高的100名的电影':movie_top100,
                     '评分最低的100名的电影':movie_bottom100,
                    }
    return data_assemblage

'''def Plotting(datas):
    datas[]'''

if __name__=='__main__':
    cookie=getCookie()
    proxies={'https':'https://127.0.0.1:1080',
             'http':'http://127.0.0.1:1080'}
    header={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 '
            'Safari/537.36','Referer':'https://movie.douban.com/tag/',
            'Host':'movie.douban.com',
            'Cookie':cookie
            }
    
    information=spider()
    #SaveToExcel(information)
    #SaveToSQL3(information)
    assemblage=DataAnalysis(information)
    #Plotting(assemblage)