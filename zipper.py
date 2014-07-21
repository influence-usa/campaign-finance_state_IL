from boto.s3.connection import S3Connection
from boto.s3.key import Key
from cStringIO import StringIO
import zipfile
import os
from datetime import datetime

AWS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET = os.environ['AWS_SECRET_KEY']

if __name__ == "__main__":
    conn = S3Connection(AWS_KEY, AWS_SECRET)
    bucket = conn.get_bucket('il-elections')
    outp = StringIO()
    now = datetime.now().strftime('%Y-%m-%d')
    zf_name = 'IL_Elections_%s' % now
    with zipfile.ZipFile(outp, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        for f in bucket.list():
            if not f.name.startswith('backups') and not f.name.endswith('/'):
                print 'adding %s' % f.name
                now = datetime.now().strftime('%Y-%m-%d')
                info = zipfile.ZipInfo('%s/%s' % (zf_name, f.name))
                info.external_attr = 0644 << 16L
                zf.writestr(info, f.get_contents_as_string())
    k = Key(bucket)
    k.key = '%s.zip' % zf_name
    k.set_contents_from_file(outp)
    k.make_public()
   #with open('test.zip', 'wb') as f:
   #    f.write(outp.getvalue())
