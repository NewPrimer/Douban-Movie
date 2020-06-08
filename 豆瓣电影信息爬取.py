import os
import pandas
import pickle
import sqlite3
import requests
import matplotlib
import matplotlib.pyplot as plt
from re        import findall
from math      import log
from time      import sleep
from lxml      import etree
from copy      import deepcopy
from functools import reduce

def getInteger(s):
    return int(findall(r'\d+',s)[0])

def getName(s):
    return findall(r'[\u4e00-\u9fa5|·|\-|A-Za-z| ]+',s)

'''def getCookie():
    f=open("cookie.txt","r")
    string=f.read()
    f.close()
    return string'''

def MinAndMax(a_list):
    a=int(min(a_list)/10)*10
    b=int(max(a_list)/10)*10+10
    return a,b

def SaveValue(value,FileName):
    f=open(FileName,'wb')
    pickle.dump(value,f)
    f.close()

def LoadValue(FileName):
    f=open(FileName,'rb')
    value=pickle.load(f)
    f.close()
    return value

def CreateFolder(FolderName):
    folder_path='./'+FolderName+'/'
    if os.path.exists(folder_path)==False:  #判断文件夹是否已经存在
        os.makedirs(folder_path)  #创建文件夹

def histogram(datas,photoName,size_x=20,size_y=20,dpi=360,xlabel='',ylabel=''): #绘制条形图
    matplotlib.rcParams['font.sans-serif']=['SimHei']
    matplotlib.rcParams['axes.unicode_minus']=False
    matplotlib.rcParams['figure.figsize']=(size_x,size_y)
    
    x_axis=[k[0] for k in datas]
    y_axis=[k[1] for k in datas]
    min_lim,max_lim=MinAndMax(y_axis)
    
    length=len(x_axis)
    plt.barh(range(length),y_axis,height=0.7,color='steelblue',alpha=0.8) #从下往上画
    plt.yticks(range(length),x_axis)
    plt.xlim(min_lim,max_lim)
    plt.title(photoName)
    
    #给X轴和Y轴命名
    if xlabel!='':
        plt.xlabel(xlabel)
    if ylabel!='':
        plt.ylabel(ylabel)
    
    for x,y in enumerate(y_axis):
        plt.text(y+0.2,x-0.1,'%s'%y)
    plt.savefig('数据分析图表/'+photoName+'.png',dpi=dpi)
    plt.show()

def PieChart(datas,photoName,size_x=20,size_y=20,dpi=360): #绘制饼状图
    matplotlib.rcParams['font.sans-serif']=['SimHei']
    matplotlib.rcParams['axes.unicode_minus']=False
    matplotlib.rcParams['figure.figsize']=(size_x,size_y)
    
    label_list=[k[0] for k in datas]
    size=[k[1] for k in datas]
    patches,l_text,p_text=plt.pie(size,labels=label_list,labeldistance=1.1,\
                          autopct="%1.1f%%",shadow=False,startangle=90,\
                          pctdistance=0.6)
    
    #调整字体大小
    for i in p_text:
        i.set_size(15)
    for i in l_text:
        i.set_size(15)
    
    #绘图
    plt.axis("equal") #设置横轴和纵轴大小相等，这样饼才是圆的
    plt.legend()
    plt.savefig('数据分析图表/'+photoName+'.png',dpi=dpi)
    plt.show()

def LineChart(datas,photoName,size_x=14,size_y=14,dpi=360,xlabel='',ylabel=''): #绘制折线图
    matplotlib.rcParams['font.sans-serif']=['SimHei']
    matplotlib.rcParams['axes.unicode_minus']=False
    matplotlib.rcParams['figure.figsize']=(size_x,size_y)
    
    #生成横纵坐标相关数据
    x_axis=[i[0] for i in datas[0][1]]
    for i in datas:
        y_axis=[j[1] for j in i[1]]
        plt.plot(x_axis,y_axis,marker='s',label=i[0])
    
    #给X轴和Y轴命名
    if xlabel!='':
        plt.xlabel(xlabel)
    if ylabel!='':
        plt.ylabel(ylabel)
    
    #绘图
    plt.legend(loc='upper left')
    plt.title(photoName)
    plt.savefig('数据分析图表/'+photoName+'.png',dpi=dpi)
    plt.show()

def spider():
    limit=100 #limit乘以20即为要爬取的电影数量
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
            sleep(5)
        
        SaveValue(i,fn_i) #记录爬取进度
        SaveValue(err,fn_err) #记录错误次数
        SaveValue(information,filename) #将存储电影信息的列表保存至文件中
    
    return information

def SaveToExcel(total):
    print('正在保存至Excel表格文件......')
    ff=pandas.DataFrame(total)
    ff.to_excel('豆瓣热门电影.xls')

def SaveToSQL3(datas):
    print('正在保存至SQLite3数据库文件......')
    
    flag=os.path.exists("./豆瓣热门电影.db") #数据库文件存在标记
    
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
    def SingleQuote(string):
        #写入的字符串可能包含单引号导致写入错误，故将单引号转换成全角形式
        string=string.replace('\'','‘')
        return string
    for i in datas:
        #注意sql语句中使用了格式化输出的占位符来表示将要插入的变量，其中%s需要加引号''
        sql="insert into cname(title,casts,directors,genres,country,lang,"\
            "year,date,time,star,rating_people,introduction,url)values"\
            "('%s','%s','%s','%s','%s','%s',%d,'%s',%d,%f,%d,'%s','%s')"\
            %(SingleQuote(i['电影名称']),SingleQuote(i['主演']),
              SingleQuote(i['导演']),i['影片类型'],i['制片国家/地区'],
              i['语言'],i['年份'],i['上映日期'],i['片长'],i['豆瓣评分'],
              i['评论人数'],SingleQuote(i['简介']),i['链接'])
        conn.execute(sql)
        conn.commit()
    
    #关闭数据库连接
    conn.close()

def getHTMLtext(url):
    try:
        #返回json，在浏览器审查，network当中的xrh里面可以检测到，用到了JSONView浏览器插件
        r=requests.get(url,headers=header)
        r.raise_for_status()
        return r.json()
    except:
        return print('爬取过程中出现异常')

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
    country=findall(r'制片国家/地区:</span>(.*?)<br/>',html)
    #语言
    lang=findall(r'语言:</span>(.*?)<br/>',html)
    #简介
    introduction=getInfo("//span[@property='v:summary']/text()")
    
    #转化成合适的类型
    def tran(string):
        if string=='NotDefined':
            string='未知'
        return string
    try:
        star=float(star)
        director=tran(str(director))
        rating_people=int(rating_people)
        time=tran(getInteger(time))
        year=getInteger(year)
        country=tran((country[0].split())[0])
        lang=tran((lang[0].split())[0])
        introduction=(introduction.split())[0]
        if introduction=='NotDefined':
            introduction='无'
        if country=='中国':
            country='中国大陆'
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

def DataAnalysis(datas):
    #创建列表用于统计所有出现过的国家、导演和电影类型
    countries=[]
    directors=[]
    movieType=[]
    
    #提取所有的国家、导演和影片类型
    for i in datas:
        def collect(data,word):
            types=getName(word)
            for j in types:
                if (j in data)==False:
                    data.append(j)
        
        collect(directors,i['导演'])
        collect(movieType,i['影片类型'])
        collect(countries,i['制片国家/地区'])
    
    #生成排行榜
    moiveRank=[[i['电影名称'],i['豆瓣评分']] for i in datas]
    moiveRank.sort(key=lambda x:x[1],reverse=True)
    movie_top50=moiveRank[:50][::-1]
    movie_bottom50=moiveRank[-50:]
    longest_movie=[[i['电影名称'],i['片长']] for i in datas]
    longest_movie.sort(key=lambda x:x[1],reverse=True)
    longest_movie=longest_movie[:50][::-1]
    
    #对各元素进行拓展
    cou=[[i,0] for i in countries]
    country_9=deepcopy(cou)
    y=[[i,0,0,0,0] for i in countries]
    type_9=[[i,0] for i in movieType]
    director_9=[[i,0] for i in directors]
    countries=[[i,0,0,0] for i in countries]
    directors=[[i,0,0,0,0,0] for i in directors]
    
    #统计各个国家和导演的电影数据
    def acc(src,dest):
        for j in getName(src):
            for k in dest:
                if k[0]==j:
                    k[1]+=1
    for i in datas:
        if i['豆瓣评分']>=9:
            for j in country_9:
                if j[0]==i['制片国家/地区']:
                    j[1]+=1
            acc(i['影片类型'],type_9)
            acc(i['导演'],director_9)
        for j in getName(i['制片国家/地区']):
            for k in countries:
                if k[0]==j:
                    k[1]+=i['豆瓣评分']
                    k[2]+=1
                    break
        for j in getName(i['导演']):
            for k in directors:
                if k[0]==j:
                    k[1]+=i['豆瓣评分']
                    k[2]+=1
                    k[4]+=i['评论人数']
                    break
    
    #为各个国家和导演计算平均豆瓣评分，保留两位小数
    for i in range(len(countries)):
        countries[i][3]=round(countries[i][1]/countries[i][2],2)
    for i in range(len(directors)):
        directors[i][3]=round(directors[i][1]/directors[i][2],2)
    
    #评价各位导演的影响力
    max_star=max([i[3] for i in directors])
    max_movie=max([i[2] for i in directors])
    max_rating_people=max([i[4] for i in directors])
    for i in directors:
        a=0.2*log(i[3]+1)/log(max_star+1)
        b=0.5*log(i[2]+1)/log(max_movie+1)
        c=0.3*log(i[4]+1)/log(max_rating_people+1)
        i[5]=round(100*(a+b+c),2)
    directors.sort(key=lambda x:x[5],reverse=True) #对各位导演按影响力进行排序
    directors_top50=[[i[0],i[5]] for i in directors[:50][::-1]]
    
    #各个年代各国电影发展状况评分
    all_years=[]
    a_years=deepcopy(y)
    years=[[i['制片国家/地区'],i['年份'],i['豆瓣评分'],i['评论人数']] for i in datas]
    years.sort(key=lambda x:x[1]) #按年代从远到近排序
    years_min=int(years[0][1]/10)*10 #最久远的年代
    def avg_star():
        #计算各国在该年代的电影平均分
        for j in range(len(a_years)):
            if a_years[j][1]!=0:
                a_years[j][2]=round(a_years[j][2]/a_years[j][1],2)
        
        #计算该年代各国电影影响力评分
        max_star=max([i[2] for i in a_years])
        max_movie=max([i[1] for i in a_years])
        max_rating_people=max([i[3] for i in a_years])
        for j in a_years:
            a=0.2*log(j[2]+1)/log(max_star+1)
            b=0.5*log(j[1]+1)/log(max_movie+1)
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
        for j in a_years:
            if j[0]==i[0]:
                j[1]+=1
                j[2]+=i[2]
                j[3]+=i[3]
    avg_star()
    
    #各个年代各国在某一方面变化情况
    def getData(index):
        temp=deepcopy(cou)
        years_sort=deepcopy(all_years)
        for i in years_sort:
            for j in range(len(i[1])):
                i[1][j]=[i[1][j][0],i[1][j][index]]
        years_sort=[[i[0],dict(i[1])] for i in years_sort]
        for i in range(len(temp)):
            temp[i][1]=[[j[0],0] for j in years_sort]
            for j in range(len(temp[i][1])):
                temp[i][1][j][1]=years_sort[j][1][temp[i][0]]
            temp[i]+=[sum([j[1] for j in temp[i][1]])]
        temp.sort(key=lambda x:x[2],reverse=True)
        temp=temp[:10]
        return temp
    star_sort=getData(2)
    number_sort=getData(1)
    influence_sort=getData(4)
    
    #对拥有豆瓣评分大于9分的电影的数量最多的类型、国家和导演按从高到低排序
    def Filter(data):
        l=len(data)
        i=num_NotDefined=0
        sum_9=sum([i[1] for i in data])
        while i<l:
            if data[i][0]=='未知':
                l-=1
                num_NotDefined=data[i][1]
                del data[i]
            else:
                if data[i][1]/sum_9<0.01: #占比小于1%则归类为“其他”
                    data[i][0]='其他'
                    data[i][1]=sum([j[1] for j in data[i:]])+num_NotDefined
                    data=data[:i+1]
                    break
                i+=1
        return data
    type_9.sort(key=lambda x:x[1],reverse=True)
    country_9.sort(key=lambda x:x[1],reverse=True)
    director_9.sort(key=lambda x:x[1],reverse=True)
    type_9=Filter(type_9)
    country_9=Filter(country_9)
    director_9=Filter(director_9)
    
    data_assemblage={'最长的50部电影':longest_movie,
                     '评分最高的50部电影':movie_top50,
                     '评分最低的50部电影':movie_bottom50,
                     '最具影响力的50位导演':directors_top50,
                     '评分超过9分的电影的主要类型':type_9,
                     '评分超过9分的电影的国家分布':country_9,
                     '评分超过9分的电影的导演分布':director_9,
                     '10个国家及地区的电影数量情况':number_sort,
                     '10个国家及地区的电影评分变化情况':star_sort,
                     '10个国家及地区的电影影响力变化情况':influence_sort,
                    }
    return data_assemblage

def Plotting(datas):
    CreateFolder('数据分析图表')
    histogram(datas['最长的50部电影'],'最长的50部电影',xlabel='时长（分钟）')
    histogram(datas['评分最高的50部电影'],'评分最高的50部电影')
    histogram(datas['评分最低的50部电影'],'评分最低的50部电影')
    histogram(datas['最具影响力的50位导演'],'最具影响力的50位导演',xlabel='影响力评分')
    PieChart(datas['评分超过9分的电影的主要类型'],'评分超过9分的电影的主要类型')
    PieChart(datas['评分超过9分的电影的国家分布'],'评分超过9分的电影的国家分布')
    PieChart(datas['评分超过9分的电影的导演分布'],'评分超过9分的电影的导演分布')
    LineChart(datas['10个国家及地区的电影数量情况'],'10个国家及地区的电影数量情况',\
              xlabel='年代',ylabel='电影数量')
    LineChart(datas['10个国家及地区的电影评分变化情况'],'10个国家及地区的电影评分变化情况',\
              xlabel='年代',ylabel='平均豆瓣评分')
    LineChart(datas['10个国家及地区的电影影响力变化情况'],'10个国家及地区的电影影响力变化情况',\
              xlabel='年代',ylabel='影响力评分')
    
    print('数据分析结果已保存至文件夹“数据分析图表”下。')

if __name__=='__main__':
    proxies={'https':'https://127.0.0.1:1080',
             'http':'http://127.0.0.1:1080'}
    header={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
            'Referer':'https://movie.douban.com/tag/',
            'Host':'movie.douban.com',
            #'Cookie':cookie
            }
    
    information=spider()
    SaveToExcel(information)
    SaveToSQL3(information)
    assemblage=DataAnalysis(information)
    Plotting(assemblage)