import sys
import json
import getopt
import logging
import getpass

FORMAT = "%(asctime)-15s [%(levelname)s] %(message)s"
logging.basicConfig(format=FORMAT)

log = logging.getLogger(sys.argv[0])

commands = dict()
commands["?"]="show available commands"
commands["help"]="show help for available commands"
commands["open"]="open connection to CDMI server"
commands["close"]="close connection to CDMI server"
commands["query"]="make CDMI query"
commands["qos"]="manage QoS for CDMI object"
commands["exit"]="exit client"
commands["quit"]="quit client"
commands["auth"]="authentication to the CDMI server"

# global variables
host=    ''
port=    80
user=    ''
password=''
token=   ''
debug=   False

def usage():
  print 'CDMI interactive command line client'
  print 
  print '(C) Karlsruhe Institute of Technology (KIT)'
  print '       Steinbuch Centre for Computing (SCC)'
  print
  print 'usage: {0} [options]'.format(sys.argv[0])
  print '-h --help    - show this message'
  print '-s --server  - connect to server'
  print '-p --port    - connect to server:port'
  print '-d --debug   - enable debug mode'
  print
  print 'example: {0}'.format(sys.argv[0])
  print 'example: {0} --debug'.format(sys.argv[0])
  print 'example: {0} -s cdmi.example.com -p 443'.format(sys.argv[0])
  
def help():
  print 'available commands:'
  for cmd in commands:
    print '{0:{width}} - {1:{width}}'.format(cmd, commands[cmd], width=6)

def help_open():
  print 'open   - opens a new connection to the CDMI server at host:port'
  print '         default port = 80'
  print
  print 'usage:   open host [port]'
  print
  print 'example: open cdmi.example.com'
  print 'example: open cdmi.example.com 443'

def help_query():
  print 'query  - makes a CDMI query to the CDMI object at the given path'
  print
  print 'usage:   query path [json | all]'
  print '         all  - show all information'
  print '         json - show the raw JSON response'
  print
  print 'example: query cdmi_capabilities raw'
  print 'example: query cdmi_capabilities/DiskOnly'
  print 'example: query test.txt all'

def help_qos():
  print 'qos    - managed the QoS of the CDMI object at the given path'
  print
  print 'usage:   qos path capabilitiesUri'
  print
  print 'example: qos test.txt TapeOnly'

def help_auth():
  print 'auth   - authentication for the CDMI server'
  print
  print 'usage:   auth [basic | oidc] [token]'
  print
  print 'example: auth basic'
  print 'example: auth oidc eyJraWQiOiJyc2ExIiw'

def help_help():
  print 'help   - show help for command'
  print
  print 'usage:   help command'
  print
  print 'example: help open'

def print_response(json_, format_=None):
  if format_ == "json":
    print json.dumps(json_, indent=4)
    return

  object_type = json_.get('objectType')

  print 'Object name:            {0}'.format(json_.get('objectName'))
  print 'Object type:            {0}'.format(json_.get('objectType'))
  print 'Object id:              {0}'.format(json_.get('objectID'))
  print 'Parent URI              {0}'.format(json_.get('parentURI'))
  print 'Parent id:              {0}'.format(json_.get('parentID'))

  if object_type == "application/cdmi-container":
    print 'Capabilities URI:       {0}'.format(json_.get('capabilitiesURI'))
    print 'Domain URI:             {0}'.format(json_.get('domainURI'))
    print 'Completion status:      {0}'.format(json_.get('completionStatus'))

  elif object_type == "application/cdmi-object":
    print 'Capabilities URI:       {0}'.format(json_.get('capabilitiesURI'))
    print 'Domain URI:             {0}'.format(json_.get('domainURI'))
    print 'Completion status:      {0}'.format(json_.get('completionStatus'))
    print 'MIME type:              {0}'.format(json_.get('mimetype'))

  elif object_type == "application/cdmi-capability":
    if json_.has_key('children'):
      print 'Children:                  '
      for child in json_.get('children', []):
        print '                        {0}'.format(child)  
    if json_.has_key('metadata') and json_.get('metadata'):
      print 'Capabilities:              '
      print '  Data redundancy:      {0}'.format(json_.get('metadata', dict()).get('cdmi_data_redundancy'))
      print '  Latency (ms):         {0}'.format(json_.get('metadata', dict()).get('cdmi_latency'))
      print '  Geographic placement: {0}'.format(json_.get('metadata', dict()).get('cdmi_geographic_placement'))
      print '  Throughput (bps):     {0}'.format(json_.get('metadata', dict()).get('cdmi_throughput'))

def auth(oidc=None):
  global user
  global password
  global token

  if oidc:
    token = oidc
  else:
    user = raw_input('Enter username: ')
    password = getpass.getpass('Enter password: ')

def query(path, option):
  import requests
  from requests.packages.urllib3.exceptions import InsecureRequestWarning
  requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

  if not host:
    print "!error: you need to open a connection to a CDMI server first"
    return

  path = path.strip().lstrip('/')

  r = None
  headers = {'X-CDMI-Specification-Version': '1.1.1'}
  try:
    if user:
      r = requests.get('https://{0}:{1}/{2}'.format(host, port, path), verify=False, auth=(user,password), headers=headers)
    elif token:
      headers['Authentication'] = 'Bearer {0}'.format(token)
      r = requests.get('https://{0}:{1}/{2}'.format(host, port, path), verify=False, headers=headers)
    else:
      print "!error: you need to provide authentication first"
      return
  except requests.exceptions.ConnectionError:
    try:
      if user:
        r = requests.get('http://{0}:{1}/{2}'.format(host, port, path), auth=(user,password), headers=headers)
      elif token:
        headers['Authentication'] = 'Bearer {0}'.format(token)
        r = requests.get('http://{0}:{1}/{2}'.format(host, port, path), verify=False, headers=headers)
      else:
        print "!error: you need to provide authentication first"
        return
    except requests.exceptions.ConnectionError:
      print "!connection error"
      return

  log.info("status code: {}".format(r.status_code))
  log.info("response headers {}".format(r.headers))

  if r.status_code == 401:
    print "!not authorized"
  elif r.status_code == 404:
    print "!not found"
  elif r.status_code == 200:
    j = r.json()
    log.info(j)
    print_response(j, option)

def qos(path, capabilities):
  pass

def main():
  global host
  global port
  global debug

  # read the commandline options
  try:
    opts, args = getopt.getopt(sys.argv[1:],"hds:p:",["help","debug","server","port"])
  except getopt.GetoptError as err:
    usage()
    sys.exit(0)
 
  for o,a in opts:
    if o in ("-h","--help"):
      usage()
      sys.exit(0)
    elif o in ("-d","--debug"):
      debug = True
    elif o in ("-s", "--server"):
      host = a
    elif o in ("-p", "--port"):
      port = int(a)
    else:
      assert False,"Unhandled Option"

  if debug:
    log.setLevel(logging.DEBUG)

  log.info("started with host {0}, port {1}".format(host, port))

  try:
    while True:
      cmd = raw_input('cdmi @{0}> '.format(host))
      cmd = cmd.strip()

      if len(cmd.split()) == 1:
        if cmd == "?":
          help()
        elif cmd == "quit":
          sys.exit(0)
        elif cmd == "exit":
          sys.exit(0)
        elif cmd == "open":
          help_open()
        elif cmd == "close":
          host=''
        elif cmd == "query":
          help_query()
        elif cmd == "qos":
          help_qos()
        elif cmd =="help":
          help_help()
        elif cmd == "auth":
          help_auth()
        else:
          print '!unknown command {0}'.format(cmd)

      # multi arg commands
      elif len(cmd.split()) > 1:
        args = cmd.split()

        # open command
        if args[0] == "open":
          if len(args) == 3:
            port = args[2]
          host = args[1]
          log.info('open {0} {1}'.format(host, port))

        # auth command
        elif args[0] == "auth":
          if args[1] == "basic":
            log.info('auth {0} {1}'.format(args[0], args[1]))
            auth()
          elif args[1] == "oidc":
            if len(args) != 3:
              print '!error: you need to provide a oidc token'
              help_auth()
            else:
              log.info('auth {0} {1}'.format(args[1], args[2]))
              auth(args[2])
          else:
            print '!error: unknown options {0}'.format(''.join(args))

        # query command
        elif args[0] == "query":
          path = args[1]
          output = 'default'
          if len(args) == 3:
            output = args[2]
          log.info('query {} {}'.format(path, output))
          query(path, output)

        # qos command
        elif args[0] == "qos":
          if len(args) != 3:
            print '!error: you need to provide a capabilities uri'
            help_qos()
          else:
            path = args[1]
            capabilities = args[2]
            log.info('qos {} {}'.format(path, capabilities))
            qos(path, capabilities)

        # all the help commands
        elif args[0] == "help":
          if args[1] == "help":
            help_help()
          elif args[1] == "?":
            print commands["?"]
          elif args[1] == "quit": 
            print commands["quit"]
          elif args[1] == "exit":
            print commands["exit"]
          elif args[1] == "open":
            help_open()
          elif args[1] == "close":
            print commands["close"]
          elif args[1] == "query":
            help_query()
          elif args[1] == "qos":
            help_qos()
          elif args[1] == "auth":
            help_auth()
          else:
            print '!unknown command {0}'.format(''.join(args))
        else:
          print '!unknown command {0}'.format(''.join(args))
      else:
        help()

  except KeyboardInterrupt:
    sys.exit(0)

if __name__ == "__main__":
  main()
