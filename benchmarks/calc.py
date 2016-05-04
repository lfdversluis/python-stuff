f = open("../data/data.txt", "r")

sync = {}
async = {}

for lines in f.readlines():
    chunks = lines.split(" ")
    if chunks[0] == "asynchronous":
        async[chunks[1]] = chunks[2]
    else:
        sync[chunks[1]] = chunks[2]

count = 0
gross = 0

for key in async:
    count += 1
    gross += float(async[key]) / float(sync[key]) * 100
    print str(float(async[key]) / float(sync[key])) + "%"

print "total: %s" % str(gross / count)
