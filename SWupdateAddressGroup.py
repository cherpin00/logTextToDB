import sys
import json
import datetime
sys.path.append(".\pySonicOSapi")
from SonicWall import SonicWall
from AddressObject import AddressObjectWithParams


def createName(strJson:str, groupName:str):
    myDict=json.loads(strJson)
    srcip=myDict["src_ip"]
    firstDateOfAttack=myDict.get("firstDateOfAttack", "unknown")
    lastThreat=myDict.get("lastDateOfAttack", "unknown").replace(" ", "_")
    created=str(datetime.datetime.now()).replace(" ", "_")
    updated=created  #eg. 2020-02-03 12:30:00.123
    lastProtocol=myDict.get("protocol", "unknown")
    numOccur=1

    name=f"AUTO_{groupName}_{srcip};updated={updated};created={created};numOccur={numOccur};desc=lastThreat:{lastThreat},lastProt:{lastProtocol}"
    print(f"Name={name}")
    return name
def getItem(strJson:str, key, default=None, mustExist=False):
    # Default is ignored if mustExist=True
    try:
        myDict=json.loads(strJson)
        if not mustExist:
            return myDict.get(key, default)
        else:
            return myDict[key]
    except:
        raise RuntimeError(f"An error occurred in getItem. str:{str}, key:{key}")
# if __name__ == "__main__":
#     groupName="AUTO_mdaemonBlacklist"
#     groupName="my_AUTO_Blacklist"
#     # createName('{"src_ip":"1.1.1.1", "name":"caleb"}', groupName)
#     # createName("{'src_ip': '65.166.136.117', 'firstDateOfAttack': '2020-12-19 01:20:52.310', 'lastDateOfAttack': '2020-12-19 06:24:49.512'}".replace("'",'"'), groupName)

#     sys.stdin = open("input2.txt", "r")

#     sw = SonicWall.connectToSonicwall("192.168.71.3")
#     keys = {"ip":"", "lastThreat":"", "updated":"", "lastProt":"", "created":"", "numOccur":""}
#     while True:
#         myStr=input()
#         addrName=createName(myStr, groupName)
#         ip=getItem(myStr, "src_ip", mustExist=True)
#         addrObject=sw.getAddressObjectStartingWith_First(f"{groupName}_{ip}")
#         if addrObject is None:
#             addrObject=AddressObjectWithParams(addrName, ip)
#             sw.createIPv4AddressObject(addrObject)
#             group = sw.getIPv4AddressGroupByName(groupName)
#             name = addrObject.hiddenName
#             if group.addToGroupOnSonicwall(name, sw) == False:
#                 raise RuntimeError(f"Could not add {name} to group {groupName}")
#         else:
#             descr = "lastThreat:"+getItem(myStr, "lastDateOfAttack",'') + ", lastProt:" + getItem(myStr, "protocol", 'unknown')
#             sw.modifyAddressObject(addrObject, newDescription=descr, updateWithHiddenName=False)
#         sw.commit()

def getAddrObj(strJson, groupName, keys):
    myDict=json.loads(strJson)
    srcip=myDict["src_ip"]
    firstDateOfAttack=myDict.get("firstDateOfAttack", "unknown")
    lastThreat=myDict.get("lastDateOfAttack", "unknown").replace(" ", "_")
    created=str(datetime.datetime.now()).replace(" ", "_")
    updated=created  #eg. 2020-02-03 12:30:00.123
    lastProtocol=myDict.get("protocol", "unknown")
    numOccur=1

    addrObj = AddressObjectWithParams("", "")
    addrObj.prefixName = groupName
    addrObj.keys = keys
    #Object member variables must match keys
    addrObj.ip = srcip
    addrObj.lastThreat = lastThreat
    addrObj.updated=updated
    addrObj.lastProt = lastProtocol
    addrObj.created=created
    addrObj.numOccur=numOccur

    addrObj.hiddenName = addrObj.getName() #This is because we want the hidden to match the generated name the first time the object is created
    return addrObj

def getDictIPv4AddressObjects(sw, prefix):
    allAddresses=sw.getArrayIPv4AddressObjects()
    dAddresses={}
    for addr in allAddresses:
        if addr.hiddenName[:len(prefix)]==prefix:
            dAddresses[addr.ip]=addr
    return dAddresses

def getAddressObjectwithIP(prefix, ip, sw, cachedDictOfAddressObjects=None):
    if cachedDictOfAddressObjects is None:
        cachedDictOfAddressObjects=getDictIPv4AddressObjects(sw, prefix)
    return cachedDictOfAddressObjects.get(ip,None)

if __name__ == "__main__":
    # groupName="my_AUTO_Blacklist"
    groupName="AUTO_mdaemonBlacklist"
    # createName('{"src_ip":"1.1.1.1", "name":"caleb"}', groupName)
    # createName("{'src_ip': '65.166.136.117', 'firstDateOfAttack': '2020-12-19 01:20:52.310', 'lastDateOfAttack': '2020-12-19 06:24:49.512'}".replace("'",'"'), groupName)
    test_newAddressObject=AddressObjectWithParams(name="", ip='1.1.1.1', zone="WAN")

    test1Json='{"src_ip": "65.166.136.118", "firstDateOfAttack": "2020-12-19 01:20:52.310", "lastDateOfAttack": "2020-12-19 06:24:49.512"}'

    # keys = {"ip":"", "lastThreat":"", "updated":"", "lastProt":"", "created":"", "numOccur":""}
    keys = {"numTotalFailedLogins":"", "lastThreat":"", "ip":"", "updated":"", "created":"", "numOccur":""}

    sw = SonicWall.connectToSonicwall("192.168.71.3")
    if sw is None:
        sw = SonicWall.connectToSonicwall("192.168.71.3")
    # sw.logoff() #if a user is logged in, the SW connection will not work, even though no errors are returned.
    if sw is None:
        raise RuntimeError("Could not login to Sonicwall.")

    x1=getAddrObj(test1Json, groupName, keys)
    x2=existingAddressObject=sw.getAddressObjectwithIP(groupName, "65.166.136.118")


    # sys.stdin = open("input2.txt", "r")

    group = sw.getIPv4AddressGroupByName(groupName)

    cachedAddrs=getDictIPv4AddressObjects(sw, groupName)

    #Todo:Set LOG_LEVEL here rather than in the logger.py file
    lineCount=0
    while True:
        try:
            myStr=input()
            lineCount+=1
            if lineCount % 10==0:
                print(f"Line Number:{lineCount}")
            print(f"Input is:{myStr}.")
            myStr=myStr.replace("'", '"')
            try:
                a_json = json.loads(myStr.replace("'",'"'))
                if myStr.strip()[:1] != "{":
                    raise RuntimeError(f"Invalid JSON: {yStr}")
                # print("Json:", a_json)
            except:
                print(f"String, {myStr} could not be converted to JSON.")
                continue
            import datetime
            from time import sleep
            # sleep(1)
        except EOFError:
            break
        # newAdressObject = getAddrObj(myStr, groupName, keys)
        ip=getItem(myStr, "src_ip", mustExist=True)
        # existingAddressObject=sw.getAddressObjectStartingWith_First(newAdressObject.getName()[:len("AUTO_") + len(groupName) + len(f";ip={ip};")])
        existingAddressObject=getAddressObjectwithIP(groupName, ip, sw, cachedAddrs)
        # if existingAddressObject is None:
        #     cachedAddrs=getDictIPv4AddressObjects(sw, groupName)
        #     existingAddressObject=getAddressObjectwithIP(groupName, ip, sw, cachedAddrs)
        type=""
        if existingAddressObject is None:
            type="new"
            newAddressObject=AddressObjectWithParams(name="", ip=ip, zone="WAN")
            newAddressObject.lastThreat=getItem(myStr,"firstDateOfAttack", "unknown")
            newAddressObject.lastProt=getItem(myStr, "lastDateOfAttack", "unknown").replace(" ", "_")
            newAddressObject.prefixName=groupName
            newAddressObject.keys=keys
            newAddressObject.hiddenName=newAddressObject.getName()
            newAddressObject.prefixName=groupName
            myAddressObject=newAddressObject
            name = newAddressObject.getName()
            sw.createIPv4AddressObject(newAddressObject, useHiddenName=False)
        else:
            type="existing"
            # descr = "lastThreat:"+getItem(myStr, "lastDateOfAttack",'') + ", lastProt:" + getItem(myStr, "protocol", 'unknown')
            existingAddressObject.keys=keys
            existingAddressObject.lastThreat = getItem(myStr, "lastDateOfAttack",'')
            existingAddressObject.lastProt = getItem(myStr, "protocol", 'unknown')
            myAddressObject=existingAddressObject
            sw.modifyAddressObject(existingAddressObject, updateWithHiddenName=False)
            name=existingAddressObject.getName()
        sw.message=""
        if not sw.commit(throwErrorOnFailure=False):
            sw.logger.critical(f"Error occurred committing {type} address object: {name}.  Message was:{sw.message}.")
        else:
            myAddressObject.hiddenName=myAddressObject.getName()
            cachedAddrs[ip]=myAddressObject
        sw.message=""
        if group.addToGroupOnSonicwall(name, sw) == False:
            sw.logger.critical(f"Could not add {type} address object, {name}, to group {groupName}.  Message was:{sw.message}.")
        else:
            sw.message=""
            if not sw.commit(throwErrorOnFailure=False):
                sw.logger.critical(f"Error occurred committing adding {type} address object, {name}, to group, {groupName}.  Message was:{sw.message}")

        # print(f"Pending changes: {sw.getPendingChanges()}")
    sw.logout()
