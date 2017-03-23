#!/usr/bin/python
import  sys
import  datetime
import file
import os
reload(sys)
sys.setdefaultencoding('utf8')
today=datetime.date.today()
cwd= os.path.expandvars('$HOME')
home=os.path.dirname(cwd)
for line in sys.stdin:
    row=unicode(line,'utf-8')
    str_array=row.split(',')
    ecode=str_array[len(str_array)-1]
    log_file_name=today.strftime("%Y%m%d")
    file_name=cwd+'/prize-'+log_file_name+'-'+ecode+'.xlsx'
    file.create_excel(file_name)
    file.write_excel(file_name,row)
