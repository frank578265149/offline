import os,sys
from xlrd import  open_workbook
from xlutils.copy import  copy
from xlwt import *
import  xlrd

def create_excel(excel_name):

    if os.path.exists(excel_name):
        message='OK. the "%s" file exists'%excel_name
       # print  message
    else:
        w=Workbook(encoding='utf-8')
        for index in range(0,8):
           ws=w.add_sheet("sheet%d"%index)
           w.save(excel_name)
def write_excel(excel_name,row):
    str_array=row.split(',')
    rb=xlrd.open_workbook(excel_name)
    r_sheet_one = rb.sheet_by_index(0)
    r_sheet_two = rb.sheet_by_index(1)
    r_sheet_three = rb.sheet_by_index(2)
    wb=copy(rb)
    sheet_one = wb.get_sheet(0)
    sheet_two = wb.get_sheet(1)
    sheet_three = wb.get_sheet(2)
    r1 = r_sheet_one.nrows
    r2 = r_sheet_two.nrows
    r3 = r_sheet_three.nrows
    print "the content write to python is " + row
  #  print " the number of sheet_one:%d,the number of sheet_two:%d,the number of sheet_three:%d"%(r1,r2,r3)
    if(r1<=60000):
        for r  in range(0,len(str_array)):
                sheet_one.write(r1,r,str_array[r])
    elif(r1>60000 & r2<=60000):
        for r  in range(0,len(str_array)):
                 sheet_two.write(r2,r,str_array[r])

    else:
        for r  in range(0,len(str_array)):
                 sheet_three.write(r3,r,str_array[r])

    wb.save(excel_name)
if __name__  ==  "__main__":
    for index in range(0,1000000):
        create_excel("english.xlsx")
        write_excel("english.xlsx",111)