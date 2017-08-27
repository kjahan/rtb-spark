import string, json, time, math
from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel


AWS_ID = "YOUR_AWS_ID"
AWS_SECRET = "Your_AWS_SECRET"
S3_BUCKET = "YOUR_S3_BUCKET"
AWS_S3 = "s3n://" + AWS_ID + ":" + AWS_SECRET + "@" + S3_BUCKET

#get app stats from bid request
def get_app_dna(line):
    bid_req = None
    app_id, app_name, pub_name, app_cat, bundle, app_url, os = '', '', '', '', '', '', ''
    key = app_id, app_name, pub_name, app_cat, bundle, app_url, os
    try:
        inx = string.find(line, '{"')
        bid_req = json.loads(line[inx:])
    except:
        pass
    if bid_req:
        try:
            app_id = bid_req["app"]["id"]
        except:
            pass
        try:
            app_name = bid_req["app"]["name"]
        except:
            pass
        try:
            pub_name = bid_req["app"]["publisher"]["name"]
        except:
            pass
        try:
            app_cat =  str(bid_req["app"]["cat"])
        except:
            pass
        try:
            bundle = bid_req["app"]["bundle"]
        except:
            pass
        try:
            app_url = bid_req["app"]["storeurl"]
        except:
            pass
        try:
            os = bid_req["device"]["os"].lower()
        except:
            pass
        key = app_id, app_name, pub_name, app_cat, bundle, app_url, os
    return key


#get app bid floor from bid request
def get_appid_bf_pair(line):
    bid_req = None
    app_id, bf = '', 0.0
    try:
        inx = string.find(line, '{"')
        bid_req = json.loads(line[inx:])
    except:
        pass
    if bid_req:
        try:
            app_id = bid_req["app"]["id"]
        except:
            pass
        try:
            bf = float(bid_req["imp"][0]["bidfloor"]) #bid floor price
        except:
            pass
    return app_id, bf


#compute standard deviation
def compute_stddev(mcs):
    values = map(lambda x: float(x), mcs)
    n, sum_values = 1.0*len(values), sum(values)
    sum_squares = sum(map(lambda x: x * x, values))
    try:
        stddev = math.sqrt(sum_squares/n - (sum_values * sum_values)/(n * n))
    except:
        stddev = 0
    return stddev


#breakdown kewords to its key value pairs for freq generation
def get_keywords_key_val(app_id, keywords):
    #"user":{"keywords":"AGE:56,GENDER:m"}
    res = keywords.split(',')
    key_vals = []
    for item in res:
        key_val = (app_id, 'keywords:' + item.lower())
        key_vals.append(key_val)
    return key_vals


#get users demographics for a given mobile app
def get_appid_demographic(line):
    app_id, bid_req, yob, agebucket, gender, keywords = '', None, None, None, None, None
    demographic = []
    try:
        inx = string.find(line, '{"')
        bid_req = json.loads(line[inx:])
    except:
        pass
    if bid_req:
        try:
            app_id = bid_req["app"]["id"]
        except:
            pass
        try:
            yob = int(bid_req["user"]["yob"])
        except:
            pass
        try:
            gender = bid_req["user"]["gender"]
        except:
            pass
        try:
            keywords = bid_req["user"]["keywords"]
        except:
            pass
    #get age
    if yob: 
        age = 2015 - yob
        try:
            agebucket = 'age:< 13'
            if age > 12 and age <= 17:
                agebucket = 'age:13-17'
            elif age > 17 and age <= 24:
                agebucket = 'age:18-24'
            elif age > 24 and age <= 34:
                agebucket = 'age:24-34'
            elif age > 34 and age <= 44:
                agebucket = 'age:34-44'
            elif age > 44 and age <= 54:
                agebucket = 'age:44-54'
            elif age > 54 and age <= 64:
                agebucket = 'age:54-64'
            elif age > 64:
                agebucket = 'age:65+'
        except:
            pass
        else:
            demographic.append((app_id, agebucket))
    #get gender
    if gender and (gender == 'm' or gender == 'f'):
        gender = 'gender:' + gender
        demographic.append((app_id, gender))
    #get keywords categories
    if keywords:
       demographic.extend(get_keywords_key_val(app_id, keywords))
    return demographic


#get (uid, appid)  --> for users app usage
def get_userid_appid(line):
    bid_req = None
    user_id, app_id = '', ''
    try:
        inx = string.find(line, '{"')
        bid_req = json.loads(line[inx:])
    except:
        pass
    if bid_req:
        try:
            app_id = bid_req["app"]["id"]
        except:
            pass
        try:
            user_id = bid_req["device"]["ext"]["idfa"]   #idfa
        except:
            pass
    return user_id, app_id


#run spark pipeline to get statistical insights for mobile apps
def collect_apps_insights(bid_req_rdd):
    #RDD's of app names --> apps impression count
    appname_impr_rdd = bid_req_rdd.map(get_app_dna) \
            .map(lambda appname: (appname, 1))
    apps_list = appname_impr_rdd.reduceByKey(lambda a, b: a + b).map(lambda x: (x[1], x[0]))  #RDD
    sorted_apps = apps_list.sortByKey(False)#.take(200)
    sorted_apps.saveAsTextFile(AWS_S3 + "/spark-logging/apps.insights")#compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"


#collect bidfloor stats per appid
def collect_bf_stats(bid_req_rdd):
    #RDD's of (app_id,bf) --> bid floor distribution
    appid_bf_rdd = bid_req_rdd.map(get_appid_bf_pair) \
            .map(lambda appid_bf: (appid_bf[0], appid_bf[1]))
    sum_cnt = appid_bf_rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    sum_cnt.saveAsTextFile(AWS_S3 + "/spark-logging/apps.bf.mean")

    bf_std = appid_bf_rdd.groupByKey().mapValues(lambda mcs: compute_stddev(mcs))
    bf_std.saveAsTextFile(AWS_S3 + "/spark-logging/apps.bf.std")


#collect apps demographics --> note that we can have duplicate demographics from the same user due to sampling!
def collect_apps_demographics(bid_req_rdd):
    appid_demog_rdd = bid_req_rdd.flatMap(get_appid_demographic) \
            .filter(lambda appid_cat: appid_cat[0] and appid_cat[1]) \
            .map(lambda appid_cat: (appid_cat[0], [appid_cat[1]])) \
            .reduceByKey(lambda a, b: a + b)
            #.groupByKey() \
            #.map(lambda keyValue : (keyValue[0], list(keyValue[1])))

    appid_demog_rdd.saveAsTextFile(AWS_S3 + "/spark-logging/apps.demog")


#a spark pipeline to get users apps usage
def collect_users_apps_usage(bid_req_rdd):
    userid_appid_rdd = bid_req_rdd.map(get_userid_appid) \
            .filter(lambda userid_appid: userid_appid[0] and userid_appid[1]) \
            .map(lambda userid_appid: (userid_appid[0], [userid_appid[1]])) \
            .reduceByKey(lambda a, b: a + b)
            #.groupByKey() \
            #.map(lambda keyValue : (keyValue[0], list(keyValue[1])))

    userid_appid_rdd.saveAsTextFile(AWS_S3 + "/spark-logging/apps.usage")


if __name__ == "__main__":
    sc = SparkContext(appName="MopubAppsInsights")
    rdd = sc.textFile(AWS_S3 + '/rtb-server/no-bidding-09-26-2015-12-*.log.gz')
    #bid request RDD
    bid_req_rdd = rdd.sample(False, 0.05, int(time.time())) \
            .map(lambda log: log.split('\n')[0]) \
            .filter(lambda line: 'BidRequest' in line)
    
    #persistence
    bid_req_rdd.persist(StorageLevel.MEMORY_ONLY)

    #RDD's of app names --> apps impression count
    collect_apps_insights(bid_req_rdd)
    
    #RDD's of (app_id,bf) --> bid floor distribution
    #collect_bf_stats(bid_req_rdd)

    #app demographics
    #collect_apps_demographics(bid_req_rdd)

    #users apps usage
    #collect_users_apps_usage(bid_req_rdd)

    #shutdown the SparkContext
    sc.stop()
