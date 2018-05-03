# Import geni alone to get error definitions
import geni

# Context functions
from geni.aggregate import FrameworkRegistry
from geni.aggregate.context import Context
from geni.aggregate.user import User

# Cloudlab and request libraries
import geni.aggregate.cloudlab as cloudlab
import geni.rspec.pg as PG
import geni.urn as urn
import geni.util as geniutil

# Geni-lib config
import geni.minigcf.config as geniconf

# Python SSH library
import paramiko

# Subprocess functions
from subprocess import Popen,PIPE,STDOUT,call

# Libraries for SQL operations, parsing, and formatting
import pymysql
import csv
import json
import xml.etree.ElementTree as ET

# System libraries
import os
import fnmatch
import sys
import getopt

# Multithreading library
import threading

# Time libraries and RNG
from random import choice as randomchoice
from time import sleep,time
import datetime

# Math library
import numpy as np

# Exceptions and traceback
import requests.exceptions
import traceback

############
### TODO ###
############
# Remove hard-coded references to users, etc...
# Move said references to command-line options

##############################
### Define sshThread class ###
##############################
class sshThread(threading.Thread):
    def __init__(self, threadID, threadName, server, portnum, uname, keyfile, dest_dir, results):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.server = server
        self.portnum = portnum
        self.uname = uname
        self.keyfile = keyfile
        self.dest_dir = dest_dir
        self.results = results
    def run(self):
        print "Starting " + self.threadName
        result = runRemoteExperiment(self.server, self.portnum, self.uname, self.keyfile, self.dest_dir)
        self.results[self.threadID] = result
        print "Exiting " + self.threadName

###################
### Print usage ###
###################
def usage():
    print 'Usage:'
    print '\tsetuptest.py -n <number_of_slices> -s <site_name>'
    print '\tsetuptest.py --nslices=<number_of_slices> --site=<site_name>'
    print '\tDefaults:  nslices=1 site=utah'

    # TODO
    # Revisit to include all command line options

#######################
### Build a context ###
#######################
def buildContext ():
    # Set up Framework
    framework = FrameworkRegistry.get("emulab-ch2")()
    framework.cert = "/users/paper67/.ssl/emulab.pem"
    framework.key = "/users/paper67/.ssl/emulab.pem"

    # Set up User
    user = User()
    user.name = "paper67"
    user.urn = "urn:publicid:IDN+emulab.net+user+paper67"
    user.addKey("/users/paper67/.ssh/cloudlab_paper67.pub")

    # Combine Framework and User into a full context
    context = Context()
    context.addUser(user)
    context.cf = framework
    context.project = "paper67-proj"

    return context

    # TODO
    # Pass in framework.cert, framework.key, username, public key, as arguments

####################################
### Get a list of all free nodes ###
####################################
def getFreeNodes(context, site):
    nfailures = 0
    maxfailures = 3
    print "Querying AM for list of resources."
    while True:
        try:
            if site == "utah":
                ad = cloudlab.Utah.listresources(context, available = False)
            elif site == "wisc":
                ad = cloudlab.Wisconsin.listresources(context, available = True)
            elif site == "clemson":
                ad = cloudlab.Clemson.listresources(context, available = True)
            else:
                print "\tInvalid Site, exiting."
                sys.exit(2)
        except Exception as e:
            nfailures += 1
            print "In getFreeNodes: " + repr(e) + " - " + str(e)
            if nfailures >= maxfailures:
                print "\tToo many errors, exiting."
                sys.exit(2)
            else:
                print "\tRetrying listresources..."
        else:
            break
    nodes = []
    print "Initial free node list:  " + str(len(ad.nodes))
    for node in ad.nodes:
        if node.available == True:
            if site == "utah":
                if 'm510' in node.hardware_types:
                    node.hw_type = 'm510'
                    nodes.append(node)
                elif 'm400' in node.hardware_types: 
                    node.hw_type = 'm400'
                    nodes.append(node)
            elif site == "wisc":
                # TODO: Limit ourselves to c220g1 and c220g2 for now
                # Other machines have too many disks, need to revisit later.
                if 'c220g1' in node.hardware_types:
                    # Temporarily exclude a certain subset
                    # wisc_exclude = ['c220g1-031125', 'c220g1-031126', 'c220g1-031127', 'c220g1-031128', 'c220g1-031129', 'c220g1-031130']
                    # if node.name not in wisc_exclude:
                    node.hw_type = 'c220g1'
                    nodes.append(node)
                elif 'c220g2' in node.hardware_types:
                    node.hw_type = 'c220g2'
                    nodes.append(node)
            elif site == "clemson":
                # TODO: Limit ourselves to c6320 and c8220 for now
                # Other machines have too many disks, need to revisit later.
                if 'c8220' in node.hardware_types:
                    node.hw_type = 'c8220'
                    nodes.append(node)
                elif 'c6320' in node.hardware_types:
                    node.hw_type = 'c6320' 
                    nodes.append(node)
        
    print "Final free node list:  " + str(len(nodes))
    return nodes


#######################################################################
### Get a list of size nslices of nodes to test from free node pool ###
#######################################################################
def chooseTestNodes(context, nodes, nslices, site):
    # Set up prelim data structures and SQL variables
    untestednodes = []
    testednodes = []
    most_recent = {}
    testnodes = []
    server = 'localhost'
    user = 'root'
    password = 'testpassword'
    database = 'env'

    print "Choosing test nodes from free node list..."

    # Attempt SQL connection, give it a few times before failing
    nTries = 0
    maxTries = 3
    while True:
        try:
            conn = pymysql.connect(host = server, user = user, passwd = password, db = database)
        except Exception as e:
            nTries += 1
            print "In insertResults: " + repr(e) + " - " + str(e)
            print "Error #" + str(nTries) + " out of " + str(maxTries) + "."
            if nTries >= maxTries:
                print "\tCould not connect to DB, exiting..."
                sys.exit(2)
            else:
                sleep(10)
                print "\tRetrying..."
        else:
            cur = conn.cursor()
            break

    # The first SQL query, getting distinct list of previously tested nodes, placing in appropriate lists
    cur.execute("SELECT DISTINCT nodeid FROM env_info WHERE site = " + conn.escape(site) + ";");
    sql_results = cur.fetchall()
    sql_results = [item for tpl in sql_results for item in tpl]
    for node in nodes:
        if node.name in sql_results:
            testednodes.append(node)
        else:
            untestednodes.append(node)

    print "Tested nodes:  " + str(len([node.name for node in testednodes]))
    print "Untested nodes:  " + str(len([node.name for node in untestednodes]))
    # For each tested node, check when the last time it was tested was.
    # TODO
    # try/except the execute
    for node in testednodes:
        sql = "SELECT timestamp, run_success FROM env_info WHERE nodeid = " + conn.escape(node.name) + " ORDER BY timestamp DESC"
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(sql)
        result = cur.fetchone()
        # If the last time it ran it was unsuccessful, don't add unless it's been a week.
        if result['run_success'] > 0:
            most_recent[node] = result
        else:
            currenttime = int(time())
            if currenttime - int(result['timestamp']) > 604800:
                most_recent[node] = result

    # Fill in the list of test nodes, prioritizing untested nodes
    while len(testnodes) < nslices:
        if len(untestednodes) > 0:
            node = randomchoice(untestednodes)
            testnodes.append([node.name, node.hw_type])
            nodes.remove(node)
            untestednodes.remove(node)
        else:
            if len(most_recent) > 0:
                last_timestamp = min(int(d['timestamp']) for d in most_recent.values())
                last_node = [k for k in most_recent if int(most_recent[k]['timestamp']) == last_timestamp][0]
                testnodes.append([last_node.name, last_node.hw_type])
                del most_recent[last_node]
                nodes.remove(last_node)
            else:
                break
    cur.close()
    conn.close()
    print "chooseTestNodes: " + str(testnodes)
    return testnodes

##########################
### Create a new Slice ###
##########################
def createSlice(context, slicename, lifetime):
    # Make slice expiration a number of minutes into the future set by lifetime
    sliceexp = datetime.datetime.utcnow() + datetime.timedelta(minutes = lifetime)
    try:
        # Attempt to create Slice with slicename and sliceexp
        context.cf.createSlice(context, slicename, exp = sliceexp)
    # In the event of any exception, consider slice unusable, return error.
    except Exception as e:
        print "In createSlice: " + repr(e) + " - " + str(e)
        print "\tDiscarding Slice " + slicename + "."
        return 1
    # Otherwise, return success.
    else:
        return 0

###########################
### Create a new Sliver ###
###########################
def createSliver(context, testnode, slicename, nodes, site, slicenum):
    # Set up initial vars
    choosenewnode = False
    nfailures = 0
    maxfailures = 3
    testnode_ret = testnode
    print "Initial testnode_ret: " + str(testnode_ret)
    
    # Main loop (So we can retry on certain failures)
    while True:
        if choosenewnode == True:
            newnode = chooseTestNodes(context, nodes, 1, site)
            testnode_ret = newnode[0]
            print "Choose New Node:  " + str(testnode_ret)
            choosenewnode = False
        # Create a Request object to start building the RSpec.
        rspec = PG.Request()
        
        # Add a raw PC to the request, and create sliver
        node = PG.RawPC(testnode_ret[0])
        if site == "utah":
            node.disk_image = urn.Image(cloudlab.Utah, "emulab-ops:UBUNTU16-64-STD")
            node.component_id = urn.Node(cloudlab.Utah, testnode_ret[0])
        elif site == "wisc":
            node.disk_image = urn.Image(cloudlab.Wisconsin, "emulab-ops:UBUNTU16-64-STD")
            node.component_id = urn.Node(cloudlab.Wisconsin, testnode_ret[0])
        elif site == "clemson":
            node.disk_image = urn.Image(cloudlab.Clemson, "emulab-ops:UBUNTU16-64-STD")
            node.component_id = urn.Node(cloudlab.Clemson, testnode_ret[0])
            
        iface = node.addInterface()
        ipaddr = "192.168.1." + str(slicenum)
        iface.addAddress(PG.IPv4Address(ipaddr, "255.255.255.0"))
        link = rspec.Link("link")
        link.addInterface(iface)
        link.connectSharedVlan("cloudlab-benchmarks")
        
        rspec.addResource(node)
        while True:
            # Attempt to create a new sliver.  If this fails too many times,
            # then ignore slice and move on.
            try:
                if site == "utah":
                    ad = cloudlab.Utah.createsliver(context, slicename, rspec)
                elif site == "wisc":
                    ad = cloudlab.Wisconsin.createsliver(context, slicename, rspec)
                elif site == "clemson":
                    ad = cloudlab.Clemson.createsliver(context, slicename, rspec)
            except geni.aggregate.pgutil.NoMappingError as e:
                # NoMappingError usually means specific node can't be used.
                # Pick another node and try again.
                nfailures += 1
                print "Error #" + str(nfailures) + " out of " + str(maxfailures) + " - (" + testnode_ret[0] + ")."
                print "In createSliver: " + repr(e) + " - " + str(e)
                if nfailures < maxfailures:
                    print "\tChoosing new node and trying again."
                    choosenewnode = True
                    break
                else:
                    print "\tToo many errors, discarding slice."
                    return "IGNORESLICE"
            except geni.aggregate.frameworks.ClearinghouseError as e:
                # Usually a result of "Slice is busy".  Sleep, and try again.
                nfailures += 1
                print "Error #" + str(nfailures) + " out of " + str(maxfailures) + "."
                print "In createSliver: " + repr(e) + " - " + str(e)
                if nfailures < maxfailures:
                    print "\tSleeping, then trying again."
                    sleep(30)
                else:
                    print "\tToo many errors, discarding slice."
                    return "IGNORESLICE"
            except requests.exceptions.ReadTimeout as e:
                # Occasionally the connection to the aggregate manager times out
                # Sleep, and try again.
                nfailures += 1
                print "Error #" + str(nfailures) + " out of " + str(maxfailures) + "."
                print "In createSliver: " + repr(e) + " - " + str(e)
                if nfailures < maxfailures:
                    print "\tSleeping, then trying again."
                    sleep(60)
                    # Sometimes ReadTimeout occurs even if AM would otherwise report
                    # a successful sliver creation.  Attempt to check this case.
                    print "\tGetting Sliver status to check if creation went through anyways..."
                    try:
                        if site == "utah":
                            res = cloudlab.Utah.sliverstatus(context, slicename)
                        elif site == "wisc":
                            res = cloudlab.Wisconsin.sliverstatus(context, slicename)
                        elif site == "clemson":
                            res = cloudlab.Clemson.sliverstatus(context, slicename)
                    except Exception as ee:
                        print "\tIn createSliver: " + repr(ee) + " - " + str(ee)
                        print "\t\tException when checking sliver status, try again"
                    else:
                        # If sliver status doesn't throw an exception, we're probably fine
                        print "\tSliver status successful, assume sliver was created"
                        return testnode_ret
                else:
                    print "\tToo many errors, discarding slice."
                    return "IGNORESLICE"
            except Exception as e:
                # Any other exceptions, fail immediately.
                # TODO: One exception type that gets raised doesn't like str(e)?
                #       Figuring out what it is...  Only happens on Wisc?
                # Removed str(e), using only traceback.print_exc(e)
                print traceback.print_exc(e)
                print "\tUnhandled exception, discarding slice."
                return "IGNORESLICE"
            else:
                # Otherwise, return nodename.
                print "END OF createSliver: " + str(testnode_ret)
                return testnode_ret

    # We shouldn't get here, but return error just in case
    print "Reached end of function without returning.  This is not supposed to happen, discarding slice."
    return "IGNORESLICE"

    # TODO
    # Set node as broken if it errors out (in certain ways)
    
##################################
### Delete a Sliver on a Slice ###
##################################
def deleteSliver(context, slicename, site):
    nfailures = 0
    maxfailures = 3
    print "Attempting to delete sliver on slice " + slicename + "..."
    while True:
        try:
            if site == "utah":
                cloudlab.Utah.deletesliver(context, slicename)
            elif site == "wisc":
                cloudlab.Wisconsin.deletesliver(context, slicename)
            elif site == "clemson":
                cloudlab.Clemson.deletesliver(context, slicename)
            else:
                print "\tInvalid Site, exiting."
                sys.exit(2)          
        except Exception as e:
            nfailures += 1
            print "In deleteSliver: " + repr(e) + " - " + str(e)
            if nfailures >= maxfailures:
                print "\tToo many errors, letting sliver expire naturally."
                break
            else:
                sleep(10)
                print "\tRetrying sliver deletion..."
        else:
            break

#####################################################
### Run experiments on remote host using Paramiko ###
#####################################################
def runRemoteExperiment(server, portnum, uname, keyfile, dest_dir):
    ssh = paramiko.SSHClient()
    sshkey = paramiko.RSAKey.from_private_key_file(keyfile)
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Spin until the machine comes up and is ready for SSH
    nTries = 0
    maxTries = 8
    print "Awaiting completion of provisioning for " + server + ", sleeping for 4 minutes..."
    sleep(240)
    while True:
        try:
            out = Popen(["nc", "-z", "-v", "-w5", server, "22"],stderr=STDOUT,stdout=PIPE)
        except:
            print "In runRemoteExperiment: " + repr(e) + " - " + str(e)
        else:
            t = out.communicate()[0],out.returncode
            if t[1] == 0:
                break
            else:
                nTries += 1
                if nTries > maxTries:
                    return "Failure"
                else:
                    print "\tConnection attempt to " + server + " timed out, retrying (" + str(nTries) + " out of " + str(maxTries) + ")..."
                    sleep(60)

    print "Node " + server + " is up, connecting via SSH."

    # SSH connect, open stdin and stdout channels
    nTries = 0
    maxTries = 3
    while True:
        try:
            ssh.connect(hostname = server, port = portnum, username = uname, pkey = sshkey)
        except Exception as e:
            nTries += 1
            print "In runRemoteExperiment: " + repr(e) + " - " + str(e)
            print "Error #" + str(nTries) + " out of " + str(maxTries) + "."
            if nTries >= maxTries:
                return "Failure"
            else:
                sleep(10)
                print "\tRetrying..."
        else:
            print "SSH connection to " + server + " successful."
            try:
                stdin, stdout, stderr = ssh.exec_command('/bin/bash -c \
                                "cd /users/' + uname + '/ && \
                                git clone https://github.com/osdi18paper67/collection.git && \
                                /users/' + uname + '/collection/run_benchmarks.sh > /users/' + uname + '/out.log"', get_pty=True)
            except Exception as e:
                print "In runRemoteExperiment: " + repr(e) + " - " + str(e)
                return "Failure"
            else:
                result = stdout.read()
                stdout.close()
                stdin.close()
                stderr.close()
                ssh.close()
            break

    # Make directory for results if it doesn't already exist, then sftp the results over.
    nTries = 0
    maxTries = 3
    while True:
        try:
            # Make directory if it doesn't exist, then erase files if any exist
            # so we don't attempt to import old files
            call(["mkdir", dest_dir], shell = False)
            files = os.listdir(dest_dir)
            for file in files:
                os.remove(os.path.join(dest_dir,file))
            call(["sftp", "-oStrictHostKeyChecking=no", "-P", str(portnum), "-i", keyfile, uname + "@" + server + ":/users/" + uname + "/*.*", dest_dir], shell = False)
        except Exception as e:
            nTries += 1
            print "In runRemoteExperiment: " + repr(e) + " - " + str(e)
            print "Error #" + str(nTries) + " out of " + str(maxTries) + "."
            if nTries >= maxTries:
                return "Failure"
            else:
                print "\tRetrying..."
        else:
            break
    return "Success"
    # TODO
    # Tweak paramiko command sequence to use variables in commands

#####################################
### Get Switch Path from Manifest ###
#####################################
def getSwitchPath(context, slicename, site):
    nfailures = 0
    maxfailures = 3
    print "Attempting to get switch path on slice " + slicename + "..."
    while True:
        try:
            if site == "utah":
                res = cloudlab.Utah.listresources(context, slicename)
            elif site == "wisc":
                res = cloudlab.Wisconsin.listresources(context, slicename)
            elif site == "clemson":
                res = cloudlab.Clemson.listresources(context, slicename)
            else:
                print "\tInvalid Site, exiting."
                sys.exit(2)          
        except Exception as e:
            nfailures += 1
            print "In getSwitchPath: " + repr(e) + " - " + str(e)
            if nfailures >= maxfailures:
                print "\tToo many errors, aborting switch path fetch..."
                return "N/A"
            else:
                sleep(10)
                print "\tRetrying manifest fetch..."
        else:
            break
    
    try:
        root = ET.fromstring(res.text)
        link = root.find('{http://www.geni.net/resources/rspec/3}link')
        switchpath = link.find('{http://www.protogeni.net/resources/rspec/ext/emulab/1}switchpath')
    except Exception as e:
        print "In getSwitchPath: " + repr(e) + " - " + str(e)
        print "/tCould not parse Manifest, aborting switch path fetch"
        return "N/A"
    else:
        print "\t" + str(switchpath.text)
        return switchpath.text
            

###############################################
### Insert experiment results into database ###
###############################################
def insertResults(results_dir, hw_type, site, switchpath, nodename):
    print "Inserting Results into SQL database"

    # Set up SQL connection vars
    server = 'localhost'
    user = 'root'
    password = 'dbpassword'
    database = 'env'
    
    # Try to connect to the DB.  Give it a few tries before failing
    nTries = 0
    maxTries = 3
    while True:
        try:
            conn = pymysql.connect(host = server, user = user, passwd = password, db = database)
        except Exception as e:
            nTries += 1
            print "In insertResults (pymysql.connect): " + repr(e) + " - " + str(e)
            print "Error #" + str(nTries) + " out of " + str(maxTries) + "."
            if nTries >= maxTries:
                print "\tCould not connect to DB, exiting..."
                sys.exit(2)
            else:
                sleep(10)
                print "\tRetrying..."
        else:
            cur = conn.cursor()
            break
            
    ###########
    ### ENV ###
    ###########
    # Open file, save to dict
    path = results_dir + "/env_out.csv"
    envdict = {}
    try:
        with open(path) as csvfile:
            reader = csv.DictReader(csvfile)
            envdict = list(reader)[0]
    except Exception as e:
        # This is the one file we want to treat as non-negotiable.
        # If this fails, the run should be listed as failed.
        print "In insertResults (env info open): " + repr(e) + " - " + str(e)
        cur.close()
        conn.close()
        insertFailure(nodename, site)
        return "Failure"
    else:
        csvfile.close()

    # Pull out values into other variables for later use
    timestamp = envdict["timestamp"]
    nodeid = envdict["nodeid"]
    nodeuuid = envdict["nodeuuid"]
    run_uuid = envdict["run_uuid"]

    # Set table, escape values, setup SQL command and execute
    envdict["run_success"] = 1
    envdict["site"] = site
    envdict["hw_type"] = hw_type 
    # Move rest of env to the end so we can properly update if a run was "incomplete"
    
    #################
    ### DISK INFO ###
    #################   
    filelist = []
    for results_file in os.listdir(results_dir):
        if fnmatch.fnmatch(results_file, 'disk_info_*'):
            filelist.append(results_file)
        
    for results_file in filelist:
        # Open file, save to dict
        diskdict = {}
        path = results_dir + "/" + results_file
        try:
            with open(path) as csvfile:
                reader = csv.DictReader(csvfile)
                diskdict = list(reader)[0]
        except Exception as e:
            print "In insertResults (disk info open): " + repr(e) + " - " + str(e)
            print "\tFile: " + str(results_file)
            print "Submitting as an Incomplete Run"
            # Mark this as an incomplete run
            envdict["run_success"] = 2
        else:
            csvfile.close()
            # Set table, escape values, setup SQL command and execute
            table = 'disk_info'
            for k, v in diskdict.iteritems():
                diskdict[k] = conn.escape(v)
            cols = diskdict.keys()
            vals = diskdict.values()
            try:
                sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
                cur.execute(sql)
            except Exception as e:
                print "In insertResults (disk info execute): " + repr(e) + " - " + str(e)
                print "\tFile: " + str(results_file)
                print "Submitting as an Incomplete Run"
                # Mark this as an incomplete run
                envdict["run_success"] = 2
        
    #################
    ### MEM TESTS ###
    #################
    memtests = ['stream', 'membench']
    for test in memtests:
        # Open file, save to dict
        memdict = {}
        path = results_dir + "/" + test + "_info.csv"
        try:
            with open(path) as csvfile:
                reader = csv.DictReader(csvfile)
                memdict = list(reader)[0]
        except Exception as e:
            print "In insertResults (mem info open): " + repr(e) + " - " + str(e)
            # Mark this as an incomplete run
            envdict["run_success"] = 2     
        else:
            csvfile.close()
            # Insert memtest info
            table = test + "_info"
            cur = conn.cursor()
            cols = memdict.keys()
            for k, v in memdict.iteritems():
                memdict[k] = conn.escape(v)
            vals = memdict.values()
            try:
                sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
                cur.execute(sql)
            except Exception as e:
                print "In insertResults (mem info execute): " + repr(e) + " - " + str(e)
                print "\tTest: " + str(test)
                print "Submitting as an Incomplete Run"
                # Mark this as an incomplete run
                envdict["run_success"] = 2 
        
        # Insert memtest results files
        # Different results file for each socket, so look for all of them
        filelist = []
        for results_file in os.listdir(results_dir):
            if fnmatch.fnmatch(results_file, test + '_out_*'):
                filelist.append(results_file)
        
        for results_file in filelist:
            # Open file, save to dict
            memdict = {}
            path = results_dir + "/" + results_file
            try:
                with open(path) as csvfile:
                    reader = csv.DictReader(csvfile)
                    memdict = list(reader)[0]
            except Exception as e:
                print "In insertResults (mem tests open): " + repr(e) + " - " + str(e)
                print "\tFile: " + str(results_file)
                print "Submitting as an Incomplete Run"
                # Mark this as an incomplete run
                envdict["run_success"] = 2 
            else:
                csvfile.close()
                table = "mem_results"
                cur = conn.cursor()
                cols = memdict.keys()
                vals = memdict.values()
                tests_keys = [x for x in cols if "_max" in x]
                for n, key in enumerate(tests_keys):
                    tests_keys[n] = key.replace("_max", "")
                for key in tests_keys:
                    # For each test name, set up new dict with test results
                    if "_omp" in key:
                        memtestdict = dict((k,v) for k, v in memdict.iteritems() if key in k)
                        memtestdict["nthreads_used"] = memdict["omp_nthreads_used"]
                    elif "_omp" not in key:
                        memtestdict = dict((k,v) for k, v in memdict.iteritems() if key in k and not "_omp" in k)
                        memtestdict["nthreads_used"] = "1"
                    # Populate new dict with other necessary information
                    memtestdict.update({"testname":key, "run_uuid":memdict["run_uuid"], \
                                        "timestamp":memdict["timestamp"], "nodeid":memdict["nodeid"], \
                                        "nodeuuid":memdict["nodeuuid"], "units":memdict["units"], \
                                        "socket_num":memdict["socket_num"], "dvfs":memdict["dvfs"]})

                    # Escape values, setup SQL command and execute
                    cols = memtestdict.keys()
                    for n, k in enumerate(cols):
                        cols[n] = k.replace(key + "_", "")
                    for k, v in memtestdict.iteritems():
                        memtestdict[k] = conn.escape(v)
                    vals = memtestdict.values()
                    try:
                        sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
                        cur.execute(sql)
                    except Exception as e:
                        print "In insertResults (mem tests execute): " + repr(e) + " - " + str(e)
                        print "\tFile: " + str(results_file)
                        print "Submitting as an Incomplete Run"
                        # Mark this as an incomplete run
                        envdict["run_success"] = 2 
        
    ###########
    ### FIO ###
    ###########
    filelist = []
    for results_file in os.listdir(results_dir):
        if fnmatch.fnmatch(results_file, 'fio_*'):
            filelist.append(results_file)
    for results_file in filelist:
        # Open file, save to dict
        fiodict = {}
        path = results_dir + "/" + results_file
        try:
            with open(path) as csvfile:
                reader = csv.DictReader(csvfile)
                fiodict = list(reader)[0]
        except Exception as e:
            print "In insertResults (fio files open): " + repr(e) + " - " + str(e)
            print "\tFile: " + str(results_file)
            print "Submitting as an Incomplete Run"
            # Mark this as an incomplete run
            envdict["run_success"] = 2 
        else:
            csvfile.close()
            if "info" in results_file:
                table = "fio_info"
                cur = conn.cursor()
                cols = fiodict.keys()
                for k, v in fiodict.iteritems():
                    fiodict[k] = conn.escape(v)
                vals = fiodict.values()

            else:
                table = "disk_results"
                fiotestdict = {"testname":fiodict["jobname"]}
                if "read" in results_file:
                    for key, value in fiodict.iteritems():
                        if "READ_bw" in key and "agg" not in key:
                            fiotestdict[key.strip("READ_bw_")] = value
                    fiotestdict.update({"runtime":float(fiodict["READ_runtime"])/1000, "size":fiodict["READ_kb"]})
                elif "write" in results_file:
                    for key, value in fiodict.iteritems():
                        if "WRITE_bw" in key and "agg" not in key:
                            fiotestdict[key.strip("WRITE_bw_")] = value
                    fiotestdict.update({"runtime":float(fiodict["WRITE_runtime"])/1000, "size":fiodict["WRITE_kb"]})
                fiotestdict['stdev'] = fiotestdict.pop('dev')
                fiotestdict.update({"device":fiodict["device"], "units":"KB/s", "iodepth":fiodict["iod"]})

                cur = conn.cursor()
                fiotestdict.update({"run_uuid":run_uuid, "timestamp":timestamp, \
                                    "nodeid":nodeid, "nodeuuid":nodeuuid})
                for k, v in fiotestdict.iteritems():
                    fiotestdict[k] = conn.escape(v)
                cols = fiotestdict.keys()
                vals = fiotestdict.values()          
            
            try:
                sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
                cur.execute(sql)
            except Exception as e:
                print "In insertResults (fio files execute): " + repr(e) + " - " + str(e)
                print "\tFile: " + str(results_file)
                print "Submitting as an Incomplete Run"
                # Mark this as an incomplete run
                envdict["run_success"] = 2            
    
    
    #####################
    ### NETWORK TESTS ###
    #####################

    ### Ping results ###
    # Canary in the mine.  If this doesn't store properly, none of the other network
    # tests will.
    # Open file, save to dict
    pingtestdict = {}
    results_file = "ping_results.csv"
    path = results_dir + "/" + results_file
    try:
        with open(path) as csvfile:
            reader = csv.DictReader(csvfile)
            pingtestdict = list(reader)[0]
    except Exception as e:
        print "In insertResults (ping results open): " + repr(e) + " - " + str(e)
        print "Submitting as an Incomplete Run"
        # Mark this as an incomplete run
        envdict["run_success"] = 2
    else:
        csvfile.close()
        # Set table, escape values, setup SQL command and execute
        table = 'ping_results'
        for k, v in pingtestdict.iteritems():
            pingtestdict[k] = conn.escape(v)
        cols = pingtestdict.keys()
        vals = pingtestdict.values()
        try:
            sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
            cur.execute(sql)
        except Exception as e:
            print "In insertResults (ping results execute): " + repr(e) + " - " + str(e)
            print "Submitting as an Incomplete Run"
            # Mark this as an incomplete run
            envdict["run_success"] = 2
    
    ### Ping info ###
    # Open file, save to dict
    if envdict["run_success"] == 1:
        pinginfodict = {}
        results_file = "ping_info.csv"
        path = results_dir + "/" + results_file
        try:
            with open(path) as csvfile:
                reader = csv.DictReader(csvfile)
                pinginfodict = list(reader)[0]
        except Exception as e:
            print "In insertResults (ping info open): " + repr(e) + " - " + str(e)
            print "Submitting as an Incomplete Run"
            # Mark this as an incomplete run
            envdict["run_success"] = 2
        else:
            csvfile.close()
            dest_nodeid = pinginfodict["ping_dest_nodeid"]

            # Set table, escape values, setup SQL command and execute
            table = 'ping_info'
            for k, v in pinginfodict.iteritems():
                pinginfodict[k] = conn.escape(v)
            cols = pinginfodict.keys()
            vals = pinginfodict.values()
            try:
                sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
                cur.execute(sql)
            except Exception as e:
                print "In insertResults (ping info execute): " + repr(e) + " - " + str(e)
                print "Submitting as an Incomplete Run"
                # Mark this as an incomplete run
                envdict["run_success"] = 2
    
    ### iperf3 info/results ###
    if envdict["run_success"] == 1:
        results_files = ["iperf3_normal.json", "iperf3_reversed.json"]
        for results_file in results_files:
            path = results_dir + "/" + results_file
            try:
                with open(path) as jsonfile:
                    iperfdict = json.load(jsonfile)
            except Exception as e:
                print "In insertResults (iperf3 open): " + repr(e) + " - " + str(e)
                print "\tFile: " + str(results_file)
                print "Submitting as an Incomplete Run"
                # Mark this as an incomplete run
                envdict["run_success"] = 2
            else:
                jsonfile.close()
                try:
                    start = iperfdict["start"]
                    end = iperfdict["end"]
                    cpu_util = end["cpu_utilization_percent"]
                except Exception as e:
                    print "In insertResults (iperf3 results formatting): " + repr(e) + " - " + str(e)
                    print "\tFile: " + str(results_file)
                    print "Submitting as an Incomplete Run"
                    # Mark this as an incomplete run
                    envdict["run_success"] = 2
                else:
                    if "normal" in results_file:            
                        # Just insert iperf3 information once, we'll do it from the normal file
                        test_start = start["test_start"]
                        connected = start["connected"][0]
                        iperfinfodict = {"run_uuid":run_uuid, "timestamp":timestamp, \
                                         "nodeid":nodeid, "nodeuuid":nodeuuid, \
                                         "version":start["version"], "local_ip":connected["local_host"], \
                                         "local_port":connected["local_port"], \
                                         "remote_ip":connected["remote_host"], \
                                         "remote_port":connected["remote_port"], \
                                         "remote_nodeid":dest_nodeid, "protocol":test_start["protocol"], \
                                         "num_streams":test_start["num_streams"], "buffer_size":test_start["blksize"], \
                                         "omitted_intervals":test_start["omit"], "duration":test_start["duration"], "time_units":"s"}
                        table = 'iperf3_info'
                        for k, v in iperfinfodict.iteritems():
                            iperfinfodict[k] = conn.escape(v)
                        cols = iperfinfodict.keys()
                        vals = iperfinfodict.values()
                        try:
                            sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
                            cur.execute(sql)
                        except Exception as e:
                            print "In insertResults (iperf3 info execute): " + repr(e) + " - " + str(e)
                            print "\tFile: " + str(results_file)
                            print "Submitting as an Incomplete Run"
                            # Mark this as an incomplete run
                            envdict["run_success"] = 2
                             
                    # Experiment stats
                    iperftestdict = {"run_uuid":run_uuid, "timestamp":timestamp, \
                                     "nodeid":nodeid, "nodeuuid":nodeuuid, \
                                     "reverse":start["test_start"]["reverse"], "retransmits":end["sum_sent"]["retransmits"], \
                                     "local_cpu_util":cpu_util["host_total"], "remote_cpu_util":cpu_util["remote_total"]}
        
                    # Bandwidth results
                    rates = [interval['sum']['bits_per_second'] for interval in iperfdict['intervals'] if interval['sum']['omitted'] == False]
                    iperftestdict.update({"mean":np.asscalar(np.mean(rates)), "median":np.asscalar(np.median(rates)), \
                                          "min":np.asscalar(np.min(rates)), "max":np.asscalar(np.max(rates)), \
                                          "stdev":np.asscalar(np.std(rates)), "sum_sent":end["sum_sent"]["bits_per_second"], \
                                          "sum_received":end["sum_received"]["bits_per_second"], "units":"bps"})
                    table = 'iperf3_results'
                    for k, v in iperftestdict.iteritems():
                        iperftestdict[k] = conn.escape(v)
                    cols = iperftestdict.keys()
                    vals = iperftestdict.values()
                    try:
                        sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
                        cur.execute(sql)
                    except Exception as e:
                        print "In insertResults (iperf3 results execute): " + repr(e) + " - " + str(e)
                        print "\tFile: " + str(results_file)
                        print "Submitting as an Incomplete Run"
                        # Mark this as an incomplete run
                        envdict["run_success"] = 2
        

    ####################
    ### NETWORK INFO ###
    ####################
    
    # Open file, save to dict
    if envdict["run_success"] == 1:
        netdict = {}
        results_file = "net_info.csv"
        path = results_dir + "/" + results_file
        try:
            with open(path) as csvfile:
                reader = csv.DictReader(csvfile)
                netdict = list(reader)[0]
        except Exception as e:
            print "In insertResults (net info open): " + repr(e) + " - " + str(e)
            print "Submitting as an Incomplete Run"
            # Mark this as an incomplete run
            envdict["run_success"] = 2
        else:
            csvfile.close()  
            # Placeholder for when we eventually get switch paths from the manifest
            netdict["switch_path"] = switchpath
            # Set table, escape values, setup SQL command and execute
            table = 'network_info'
            for k, v in netdict.iteritems():
                netdict[k] = conn.escape(v)
            cols = netdict.keys()
            vals = netdict.values()
            try:
                sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
                cur.execute(sql)
            except Exception as e:
                print "In insertResults (net info execute): " + repr(e) + " - " + str(e)
                print "Submitting as an Incomplete Run"
                # Mark this as an incomplete run
                envdict["run_success"] = 2

    #######################
    ### ENV (CONTINUED) ###
    #######################
    table = 'env_info'
    for k, v in envdict.iteritems():
        envdict[k] = conn.escape(v)
    cols = envdict.keys()
    vals = envdict.values()
    try:
        sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
        cur.execute(sql)
    except Exception as e:
        # This is the one insertion we want to treat as non-negotiable.
        # If this fails, the run should be listed as failed.
        print "In insertResults (env info execute): " + repr(e) + " - " + str(e)
        cur.close()
        conn.close()
        insertFailure(nodename, site)
        return "Failure"
    
    #########################        
    ### COMMIT EXECUTIONS ###
    ######################### 
    # Only commit if every execution went through okay.
    try:
        conn.commit()
    except Exception as e:
        print "In insertResults (commit): " + repr(e) + " - " + str(e)
        return "Failure"
    else:
        print "SQL Insert success."
    finally:
        # Close SQL connection
        cur.close()
        conn.close()
    return "Success"

##############################################
### Insert a failed experiment run into DB ###
##############################################
def insertFailure(nodename, site):
    print "Inserting Failure state into SQL database"

    # Set up SQL connection vars
    server = 'localhost'
    user = 'root'
    password = 'testpassword'
    database = 'env'
    
    # Try to connect to the DB.  Give it a few tries before failing
    nTries = 0
    maxTries = 3
    while True:
        try:
            conn = pymysql.connect(host = server, user = user, passwd = password, db = database)
        except Exception as e:
            nTries += 1
            print "In insertResults: " + repr(e) + " - " + str(e)
            print "Error #" + str(nTries) + " out of " + str(maxTries) + "."
            if nTries >= maxTries:
                print "\tCould not connect to DB, exiting..."
                sys.exit(2)
            else:
                sleep(10)
                print "\tRetrying..."
        else:
            cur = conn.cursor()
            break
    currenttime = int(time())
    faildict = {"nodeid":nodename, "site":site, "timestamp":currenttime, "run_success":0}
    
    table = 'env_info'
    for k, v in faildict.iteritems():
        faildict[k] = conn.escape(v)
    cols = faildict.keys()
    vals = faildict.values()
    sql = "INSERT INTO %s(%s) VALUES(%s);" % (table, ",".join(cols), ",".join(vals))
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print "In insertResults: " + repr(e) + " - " + str(e)
        print "Unable to insert failure into SQL database"
    finally:
        cur.close()
        conn.close()


#####################
### Main function ###
#####################
def main(argv):
    # Default arguments
    nslices = 5
    site = "utah"

    # Parse arguments
    try:
        opts, args = getopt.getopt(argv,"hns:",["help","nslices=","site="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-n", "--nslices"):
            nslices = int(arg)
        elif opt in ("-s", "--site"):
            site = arg

    # Info for log
    print "Starting benchmarking at Cloudlab " + site + " - " + str(datetime.datetime.today())
    
    # TODO
    # Add the rest of the arguments

    # Remove cached files
    dir = "/users/paper67/.bssw/geni"
    files = os.listdir(dir)
    for file in files:
        os.remove(os.path.join(dir,file))

    # Before attempting to contact AM at all, attempt to adjust timeout
    geniconf.HTTP.TIMEOUT = 60

    # Set up context, get list of free m510 nodes
    print "Building Context, fetching list of free nodes."
    context = buildContext()
    nodes = getFreeNodes(context, site)
    # Check for number of free nodes.  Exit out early if there are none.
    if len(nodes) == 0:
        print "\tNo nodes available to run experiments on.  Exiting."
        exit(2)
    elif len(nodes) < nslices:
        nslices = len(nodes)

    # Set up node candidates and slices
    testnodes = chooseTestNodes(context, nodes, nslices, site)
    # Probably redundant, but I don't want to take any chances
    if len(testnodes) == 0:
        print "\tNo nodes available to run experiments on.  Exiting."
        exit(2)
    elif len(testnodes) < nslices:
        nslices = len(testnodes)
    slices = {}
    # Build in extra time due to new network testing
    if site == "utah":
        # Utah machines have 1 disk, set lifetime to 2 hrs 20 minutes
        # 1 disk * 8 tests * 12 minutes per test = 96 minutes, buffer time = 44 minutes
        lifetime = 140
    elif site == "wisc":
        # Wisconsin machines have 3 disks, set lifetime to 5 hrs 30 minutes
        # 3 disks * 8 tests * 12 minutes per test = 288 minutes, buffer time = 52 minutes
        # Test:  Adding more lifetime to Wisc, 7 hrs 30 minutes.
        lifetime = 450
    elif site == "clemson":
        # Clemson machines have 2 disks, set lifetime to 4 hrs
        # 2 disks * 8 tests * 12 minutes per test = 192 minutes, buffer time = 48 minutes
        lifetime = 240
    for n in range(0, nslices):
        slicename = "bench-" + site + "-" + str(n)
        testnode = testnodes[n]
        result = createSlice(context, slicename, lifetime)
        if result == 0:
            print "Node " + str(n) + " chosen and slice created - requesting Node " + testnode[0] + " in Slice " + slicename + "."
            # Sleep for 1 minute to stagger sliver creation (test)
            sleep(60)
            result = createSliver(context, testnode, slicename, nodes, site, n+1)
            if result != "IGNORESLICE":
                testnode = result
                slices[slicename] = testnode
                print "Node " + testnode[0] + " successfully requested for slice " + slicename + "."

    print str(len(slices)) + " out of " + str(nslices) + " requested slices successfully created."

    # Exit here if no slices have been created.
    if len(slices) == 0:
        print "\tNo slices available to run experiments on.  Exiting."
        exit(2)
 
    # TODO
    # Move these up to default values.  These will be command line arguments.
    portnum = 22
    uname = "paper67"
    keyfile="/users/" + uname + "/.ssh/cloudlab_" + uname
    threads = [None] * len(slices)
    thread_results = [None] * len(slices)
    for n, item in enumerate(slices.items()):
        slicename = item[0]
        nodename = item[1][0]
        threadname = nodename + "-thread"
        server = nodename + "." + site + ".cloudlab.us"
        dest_dir = "/users/" + uname + "/testresults/" + nodename
        threads[n] = sshThread(n, threadname, server, portnum, uname, keyfile, dest_dir, thread_results)
        threads[n].start()
      
    # Get the PG URL  
    sleep(480)
    for n, item in enumerate(slices.items()):
        slicename = item[0]
        nodename = item[1][0]
        try:
            if site == "utah":
                res = cloudlab.Utah.sliverstatus(context, slicename)
            elif site == "wisc":
                res = cloudlab.Wisconsin.sliverstatus(context, slicename)
            elif site == "clemson":
                res = cloudlab.Clemson.sliverstatus(context, slicename)
            print "\tSliver URL (" + str(nodename) + "): " + str(res['pg_public_url'])
        except Exception as e:
            print "In main: " + repr(e) + " - " + str(e)
            print "Unable to get sliver URL"
            
    # Wait for threads to finish
    for t in threads:
        t.join()

    # Might have an issue with the credential expiration at this point
    # We'll delete the old files and re-build the context
    dir = "/users/paper67/.bssw/geni"
    files = os.listdir(dir)
    for file in files:
        os.remove(os.path.join(dir,file))
    context = buildContext()

    # Delete slivers, insert results into DB
    nSuccesses = 0
    for n, item in enumerate(slices.items()):
        slicename = item[0]
        nodename = item[1][0]
        hw_type = item[1][1]
        dest_dir = "/users/" + uname + "/testresults/" + nodename
        if thread_results[n] == "Success":
            switchpath = getSwitchPath(context, slicename, site)
            result = insertResults(dest_dir, hw_type, site, switchpath, nodename)
            if result == "Success":
                nSuccesses += 1
        else:
            insertFailure(nodename, site)
        
        # Move deleteSliver to the end so we can get switchpath info
        deleteSliver(context, slicename, site)
        
    print str(nSuccesses) + " out of " + str(nslices) + " experiments successfully run and stored."

##################################################
### Entry point to program, call main function ###
##################################################
if __name__ == "__main__":
    main(sys.argv[1:])
else:
    print "Error, cannot enter main, exiting."
    sys.exit(2)
