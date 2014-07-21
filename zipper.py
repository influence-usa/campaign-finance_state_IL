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
    with zipfile.ZipFile(outp, mode='w') as zf:
        for f in bucket.list():
            if not f.name.startswith('backups') \
                and not f.name.endswith('/') \
                and not f.name.endswith('.zip'):
                now = datetime.now().strftime('%Y-%m-%d')
                zf.writestr('%s/%s' % (zf_name, f.name), 
                    f.get_contents_as_string(), 
                    compress_type=zipfile.ZIP_DEFLATED)
    with open('latest.zip', 'wb') as f:
        f.write(outp.getvalue())
    k = Key(bucket)
    k.key = '%s.zip' % zf_name
    outp.seek(0)
    k.set_contents_from_filename('latest.zip')
    k.make_public()
    bucket.copy_key(
        'IL_Elections_latest.zip', 
        'il-elections', 
        '%s.zip' % zf_name,
        preserve_acl=True)
