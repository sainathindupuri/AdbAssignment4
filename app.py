import string
import time
import pyodbc
import os
import redis
import timeit
import hashlib
import pickle
from flask import Flask, Request, render_template, request, flash
import random
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import io
import numpy as np
import base64

app = Flask(__name__, template_folder="templates")
connection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:adbsai.database.windows.net,1433;Database=adb;Uid=sainath;Pwd=Shiro@2018;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30')
cursor = connection.cursor()

r = redis.StrictRedis(host='adb-quiz3.redis.cache.windows.net', port=6380, db=0,
                      password='bGWVXkw0gkglji3NxJ2c4dapdnXSxI8dtAzCaKsPnF8=', ssl=True)



@app.route('/', methods=['POST', 'GET'])
def Hello():
    return render_template('index.html')

@app.route('/quakecluster', methods=['GET', 'POST'])
def quakecluster():
    earthquakes = []
    quake = []
    pie = []

    cursor.execute("select count(*) from [all_month]")
    for data in cursor:
        for value in data:
            earthquake_total = value
    print(earthquake_total)

    for i in range(-2, 8):
        cursor.execute("select * from [all_month] where Mag>="+str(i)+" and Mag<="+str(i+1))
        for data in cursor:
            quake.append(data)
        earthquakes.append(quake)
        earthquake_len = len(quake)
        pie.append((earthquake_len/earthquake_total)*100)

    labels = ["Mag((-2)-(-1))","Mag((-1)-0)","Mag(0to1)","Mag(1-2)","Mag(2-3)","Mag(3-4)","Mag(4-5)", "Mag(5-6)","Mag(6-7)","Mag(7-8)"]
    explode = [0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]
    colors = ['#ff6666', '#ffcc99', '#99ff99', '#66b3ff']
    plt.pie(pie, labels=labels, colors=colors, explode=explode, autopct='%.0f%%')
    figfile = io.BytesIO()
    plt.savefig(figfile, format='jpeg')
    plt.close()
    figfile.seek(0)
    figdata_jpeg = base64.b64encode(figfile.getvalue())
    files = figdata_jpeg.decode('utf-8')
    return render_template("magnitude_cluster.html", output = files)

@app.route('/Question10a', methods=['GET', 'POST'])
def Question10a():
    cursor = connection.cursor()    
    num1 = request.form.get("RangeStart")
    num2 = request.form.get("RangeEnd")  
    starttime = timeit.default_timer()
    query_str = "select b.id, b.net, b.place, a.nst from [ds-2] a join [dsi-1] b on a.id = b.id where a.nst >="+num1+" and a.nst <= "+num2
    cursor.execute(query_str)    
    data = cursor.fetchall()
    time = timeit.default_timer() - starttime
    return render_template('Question10a.html', data = data, time  = time)  



@app.route('/Question10b', methods=['GET', 'POST'])
def Question10b():
    cursor = connection.cursor()    
    n = request.form.get("N")
    net = request.form.get("Net")  
    off = str(random.randint(0,9))
    starttime = timeit.default_timer()
    query_str = "select top "+n+" * from (select b.id, b.net, b.place, a.nst from [ds-2] a join [dsi-1] b on a.id = b.id where b.net = '"+net+"' ORDER BY b.id OFFSET "+off+" ROWS) a1"
    cursor.execute(query_str)    
    data = cursor.fetchall()
    time = timeit.default_timer() - starttime
    return render_template('Question10a.html', data = data, time = time)  


@app.route('/Question11', methods=['GET', 'POST'])
def Question11():
    cursor = connection.cursor()

    num1 = request.form.get("RangeStart")
    num2 = request.form.get("RangeEnd")  

    n = request.form.get("N")
    net = request.form.get("Net") 
    off = str(random.randint(0,9))

    t = int(request.form.get("T")) 
    timeList1 = []
    timeList2 = []
    sum = 0
    # 10a
    for i in range(0,t):
        starttime = timeit.default_timer()
        query_str = "select b.id, b.net, b.place, a.nst from [ds-2] a join [dsi-1] b on a.id = b.id where a.nst >="+num1+" and a.nst <= "+num2
        cursor.execute(query_str)    
        data = cursor.fetchall()
        time = timeit.default_timer() - starttime
        timeList1.append(time)
        sum = sum + time

    # off = str(random.randint(0,9))
    for i in range(0,t):
        starttime = timeit.default_timer()
        query_str = "select top "+n+" * from (select b.id, b.net, b.place, a.nst from [ds-2] a join [dsi-1] b on a.id = b.id where b.net = '"+net+"' ORDER BY b.id OFFSET "+off+" ROWS) a1"
        cursor.execute(query_str)    
        data = cursor.fetchall()
        time = timeit.default_timer() - starttime
        timeList2.append(time)
        sum = sum + time

    return render_template('Question11.html', time1 = timeList1, time2= timeList2, total = sum)  

@app.route('/Question12', methods=['GET', 'POST'])
def Question12():
    cursor = connection.cursor()

    num1 = request.form.get("RangeStart")
    num2 = request.form.get("RangeEnd")  

    n = request.form.get("N")
    net = request.form.get("Net") 
    off = str(random.randint(0,9))

    t = int(request.form.get("T")) 
    timeList1 = []
    timeList2 = []
    sum = 0
    # 10a
    print( r.exists("sai"+num1+num2))
    for i in range(0,t):
        if( r.exists("sai"+num1+num2) != 1):
            starttime = timeit.default_timer()
            query_str = "select b.id, b.net, b.place, a.nst from [ds-2] a join [dsi-1] b on a.id = b.id where a.nst >="+num1+" and a.nst <= "+num2
            cursor.execute(query_str)    
            data = cursor.fetchall()
            r.set("sai"+num1+num2, pickle.dumps(data))
            time = timeit.default_timer() - starttime
            timeList1.append(time)
            sum = sum + time
        else:
            starttime = timeit.default_timer()
            data = pickle.loads(r.get("sai"+num1+num2))
            time = timeit.default_timer() - starttime
            timeList1.append(time)
            sum = sum + time

    # off = str(random.randint(0,9))
    for i in range(0,t):
        if( r.exists("sai"+n+net) != 1):
            print("not coming")
            starttime = timeit.default_timer()
            query_str = "select top "+n+" * from (select b.id, b.net, b.place, a.nst from [ds-2] a join [dsi-1] b on a.id = b.id where b.net = '"+net+"' ORDER BY b.id OFFSET "+off+" ROWS) a1"
            cursor.execute(query_str)    
            data = cursor.fetchall()
            r.set("sai"+n+net,pickle.dumps(data))
            time = timeit.default_timer() - starttime
            timeList2.append(time)
            sum = sum + time
        else:
            starttime = timeit.default_timer()
            data = pickle.loads(r.get("sai"+n+net))
            time = timeit.default_timer() - starttime
            timeList2.append(time)
            sum = sum + time

    return render_template('Question11.html', time1 = timeList1, time2= timeList2, total = sum)  



@app.route('/quakeclusterdepth', methods=['GET', 'POST'])
def quakeclusterdepth():
    quake_depth = []
    mag = []
    cursor.execute("select top 500 depth from [all_month]")
    for data in cursor:
        for value in data:
            quake_depth.append(value)

    cursor.execute("select top 500 mag from [all_month]")
    for data in cursor:
        for value in data:
            mag.append(value)

    viridis = cm.get_cmap('viridis', 12)
    colors = viridis(np.linspace(0, 1, len(quake_depth)))
    plt.scatter(quake_depth, mag, c=colors)
    plt.xlabel("Earthquake Depth")
    plt.ylabel("Earthquake Magnitude")
    plt.title("Graph of Earthquake Depth Vs Earthquake Magnitude")
    figfile = io.BytesIO()
    plt.savefig(figfile, format='jpeg')
    plt.close()
    figfile.seek(0)
    figdata_jpeg = base64.b64encode(figfile.getvalue())
    files = figdata_jpeg.decode('utf-8')

    return render_template("earthquake_magdepth.html", output = files)
    
@app.route('/quakeclustermagtype', methods=['GET', 'POST'])
def quakeclustermagtype():
    quake = []
    earthquakes = []
    limits = []
    types = []
    labels = []

    cursor.execute("select distinct Magtype from [all_month]")
    for data in cursor:
        for value in data:
            if value != None:
                types.append(value)

    for i in range(len(types)):
        cursor.execute("select * from [all_month] where Magtype=?",types[i])
        for data in cursor:
            quake.append(data)
        earthquakes.append(quake)
        earthquake_len = len(quake)
        limits.append(earthquake_len)

    cursor.execute("Select distinct Magtype from [all_month]")
    for data in cursor:
        for value in data:
            if value != None:
                labels.append(value)

    print(labels)
    plt.bar(labels, limits, color=(0.9, 0.2, 0.5, 0.2))
    plt.xlabel("Earthquake Type")
    plt.ylabel("Earthquake Count")
    plt.title("Earthquake count based on Type")
    figfile = io.BytesIO()
    plt.savefig(figfile, format='jpeg')
    plt.close()
    figfile.seek(0)
    figdata_jpeg = base64.b64encode(figfile.getvalue())
    files = figdata_jpeg.decode('utf-8')
    return render_template("magnitudetype_cluster.html", output=files)


@app.route('/quakelocation', methods=['GET', 'POST'])
def quakelocation():
    earthquakes = []
    quake = []
    plot = []
    place = request.form.get("Place")
    cursor.execute("select count(*) from [all_month]")
    for data in cursor:
        for value in data:
            earthquake_total = value
    print(earthquake_total)

    for i in range(-2, 8):
        cursor.execute("select * from [all_month] where place like'%"+place+"%'")
        for data in cursor:
            quake.append(data)
        earthquakes.append(quake)
        earthquake_len = len(quake)
        plot.append((earthquake_len/earthquake_total)*100)
    
    labels = ["Mag((-2)-(-1))","Mag((-1)-0)","Mag(0to1)","Mag(1-2)","Mag(2-3)","Mag(3-4)","Mag(4-5)", "Mag(5-6)","Mag(6-7)","Mag(7-8)"]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(labels, plot)
    plt.title('Count of Magnitude of a particular location')
    plt.xlabel('Magnitude')
    plt.ylabel('Count of earthquakes')
    figfile = io.BytesIO()
    plt.savefig(figfile, format='jpeg')
    plt.close()
    figfile.seek(0)
    figdata_jpeg = base64.b64encode(figfile.getvalue())
    files = figdata_jpeg.decode('utf-8')
    return render_template("quakelocation.html", output=files)


if __name__ == '__main__':    
    app.run()

