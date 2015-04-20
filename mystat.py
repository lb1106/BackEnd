import sys
import os
import copy
import time
import datetime
import traceback
import ConfigParser
import commands
import logging

def read_cfg(filename):
   parser = ConfigParser.ConfigParser()
   parser.readfp(open(filename))
   def read_section(section):
      items = parser.items(section)
      onesection = {}
      onesection['section'] = section
      for k, v in items:
         v = v.strip()
         if k == 'logname' :
            k = 'tablename'
            onesection[k] = v
         elif k == 'log_table' :
            fieldlist = v.split()
            fielditems = []
            for f in fieldlist:
               name, type = f.split(':')
               if f.find(':') == -1:
                  type = 'string'
               fielditems.append((name,type))
            onesection[k] = fielditems
         elif k == 'selected_field' :
            fieldlist = v.split()
            onesection[k] = fieldlist
         elif k == 'group_field' :
            fieldlist = v.split()
            onesection[k] = fieldlist
         elif k == 'stat_field' :
            fieldlist = v.split()
            fielditems = []
            for f in fieldlist:
               name, type = f.split(':')
               if f.find(':') == -1:
                  type = 'string'
               fielditems.append((name,type))
            onesection[k] = fielditems
         elif k == 'days_from_today' :
               onesection['date'] = datetime.datetime.now() - datetime.timedelta(days=int(v))
         else:
            onesection[k] = v
      return onesection
   cfglist = []
   globalcfg = read_section('global')
   for section in globalcfg['orderlist'].split():
      cfg = read_section(section)
      cfglist.append(cfg)
   return (globalcfg,cfglist)
def hive_raw_table(cfg):
   '''
   Generate hive query to create appflood-rtb-win-notify-log table partitioned by date from raw log
   '''
   fieldstring = ','.join('%s %s' %(f1, f2) for (f1, f2) in cfg['log_table'])
   tablename = cfg['tablename']
   stat_field = ','.join('%s %s' %(f1, f2) for (f1, f2) in cfg['stat_field'])
   raw_table = '''
    CREATE TABLE IF NOT EXISTS %s (%s)
    PARTITIONED BY (date string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s'
    LINES TERMINATED BY '%s' 
    LOCATION '%s';'''  %('raw_' + tablename.replace('-','_'), fieldstring, cfg['row_delimited'],cfg['line_terminated']
    ,cfg['location_path']+'raw_'+tablename.replace('-','_'))
   table = '''
    CREATE TABLE IF NOT EXISTS %s (%s)
    PARTITIONED BY (date string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s'
    LINES TERMINATED BY '%s' 
    LOCATION '%s';'''  %(  tablename.replace('-','_'), fieldstring, cfg['row_delimited'],cfg['line_terminated']
    ,cfg['location_path']+tablename.replace('-','_'))
   stat_table='''
    CREATE TABLE IF NOT EXISTS %s (%s)
    PARTITIONED BY (date string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s'
    LINES TERMINATED BY '%s' 
    LOCATION '%s';'''  %(  tablename.replace('-','_')+'_stat',stat_field, cfg['row_delimited'],cfg['line_terminated']
    ,cfg['location_path']+tablename.replace('-','_')+'_stat')
   
   partition = '''
   ALTER TABLE %s RECOVER PARTITIONS;
   ALTER TABLE %s RECOVER PARTITIONS;
   ALTER TABLE %s RECOVER PARTITIONS;
   ''' %( tablename.replace('-','_'), 'raw_'+tablename.replace('-','_'), tablename.replace('-','_')+'_stat')
   curdate = cfg['date'].strftime('%Y-%m-%d')
   load='''
   LOAD DATA LOCAL INPATH '%s' INTO TABLE %s PARTITION(date = '%s' );
   ''' %( cfg['local_path']+ curdate+'_00000','raw_'+tablename.replace('-','_'), curdate)
   return raw_table + table + stat_table + load
def hive_raw_insert(cfg):
   selectstring = ','.join(f1 for (f1,f2) in cfg['log_table'])
   stat_field = ','.join(f for f in cfg['selected_field'])
   group_field = ','.join(f for f in cfg['group_field'])
   tablename = cfg['tablename'].replace('-','_')
   curdate = cfg['date'].strftime('%Y-%m-%d')
   daybefore = (cfg['date'] - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
   nextday = (cfg['date'] - datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
   if cfg['is_first_load'] == 'true':
      daybefore = curdate
   jar_preparation = '''
   add jar /home/hadoop/TenMinute.jar;
   create temporary function tenMinute as 'TenMinute';
   '''
   insert_hive = '''
   INSERT TABLE %s PARTITION (date='%s')
   SELECT %s from %s WHERE (date='%s' or date ='%s') and  %s > '%s' and %s < '%s';
   '''  %(tablename, curdate, selectstring, 'raw_'+tablename, curdate, daybefore, cfg['time_judge_column'],curdate, cfg['time_judge_column'], nextday)
   stat_hive = '''
   INSERT TABLE %s PARTITION (date='%s')
   SELECT %s from %s WHERE (date='%s') GROUP BY %s;
   '''  %(tablename+'_stat', curdate, stat_field, tablename, curdate, group_field )
   return jar_preparation + insert_hive + stat_hive
CMD_LIST =['hive_raw']
import getopt
if __name__ =='__main__':
   (globalcfg,cfgs) = read_cfg("bingo1.cfg")
   for cfg in cfgs:
      c = copy.deepcopy(globalcfg)
      c.update(cfg)
      cfg = c
      print hive_raw_table(cfg)
      print hive_raw_insert(cfg)

   
