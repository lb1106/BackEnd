import ConfigParser
import datetime

def read_cfg( filename ):
    parser = ConfigParser.ConfigParser()
    parser.read(filename)
    def read_section(section):
        items = parser.items(section)
        onesection = {}
        onesection['section'] = section
        for k, v in items:
            k = k.strip()
            v = v.strip()
            if k == 'tablelist' or k == 'actlist' or k == 'functionlist':
                tlist = v.split()
                onesection[k] = tlist
            elif k == 'days_from_today' :
                onesection['date'] = datetime.datetime.now() - datetime.timedelta(days=int(v))
            else:
                onesection[k] =  v
        return onesection
    tablelist = []
    functionlist = []
    actlist = []
    globalcfg = read_section('global')
    for table in globalcfg['tablelist'] :
        cfg = read_section(table)
        tablelist.append(cfg)
    if globalcfg['function_add'] == 'true' :
        for f in globalcfg['functionlist']:
            cfg = read_section(f)
            functionlist.append(cfg)
    if globalcfg['moreact'] == 'true':
        for act in globalcfg['actlist']:
            cfg = read_section(act)
            actlist.append(cfg)
    return (globalcfg, tablelist, functionlist, actlist)


if __name__ == '__main__' :
    (globalcfg, tablelist, functionlist, actlist ) = read_cfg("template.cfg")
    from jinja2 import FileSystemLoader,Environment
    templateLoader = FileSystemLoader( searchpath="." )
    templateEnv = Environment( loader=templateLoader )
    template = templateEnv.get_template( 'template.hive' )
    tables=tablelist
    function_add = globalcfg['function_add']
    moreAct = globalcfg['moreact']
    functions = functionlist
    
    curdate = globalcfg['date'].strftime('%Y-%m-%d')
    daybefore = (globalcfg['date'] - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    nextday = (globalcfg['date'] - datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    for tb in tables:
        if tb['newload'] == 'true':
            tb['datapath'] = tb['datapath']+curdate+'_00000'
            tb['new_partition_date'] = curdate
    for act in actlist:
        if act['section'] == 'raw_insert' :
        	 act['condition'] = '(date=\'%s\' or date =\'%s\') and  create_time > \'%s\' and create_time < \'%s\'' %(curdate,daybefore,curdate,nextday)
        elif act['section'] == 'stat_insert' :
           act['condition'] =  '(date=\'%s\')' %curdate
        act['partition_info'] = act['partition_info']+'\'%s\'' %curdate
    Acts = actlist
    print template.render(tables=tables, function_add=function_add, functions = functions, moreAct= moreAct, Acts=Acts) 
    
     
        
            
