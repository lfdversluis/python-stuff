# coding=utf-8
import gzip
from io import BytesIO

data = unicode("ü")

file_obj = BytesIO()
g = gzip.GzipFile(fileobj=file_obj, mode='wb')
g.write(data)
g.close()
file_obj.seek(0)
zipped_data = file_obj.read()
file_obj.close()

# reconstruct it
in_memory_zip = BytesIO(zipped_data)
g = gzip.GzipFile(fileobj=in_memory_zip, mode='rb')
data = g.read()
g.close()
in_memory_zip.close()
print data
