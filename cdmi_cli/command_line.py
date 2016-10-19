import sys
import json
import getopt
import logging
import getpass

FORMAT = "%(asctime)-15s [%(levelname)s] %(message)s"
logging.basicConfig(format=FORMAT)

log = logging.getLogger('cdmi-cli')

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
  print '    Steinbuch Centre for Computing  - (SCC)'
  print
  print 'usage: {0} [options]'.format('cdmi-cli')
  print '-h --help    - show this message'
  print '-s --server  - connect to server'
  print '-p --port    - connect to server:port'
  print '-d --debug   - enable debug mode'
  print
  print 'example: {0}'.format('cdmi-cli')
  print 'example: {0} --debug'.format('cdmi-cli')
  print 'example: {0} -s cdmi.example.com -p 443'.format('cdmi-cli')
  
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
  print 'example: query cdmi_capabilities json'
  print 'example: query cdmi_capabilities/dataobject/DiskOnly'
  print 'example: query test.txt all'

def help_qos():
  print 'qos    - managed the QoS of the CDMI object at the given path'
  print
  print 'usage:   qos path capabilitiesUri'
  print
  print 'example: qos test.txt cdmi_capabilities/dataobject/DiskOnly'

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

  width = 30
  indent = 4
  indent_width = 26
  double_indent_width = 22

  object_type = json_.get('objectType')

  print '{0:{width}} {1}'.format('Object name:', json_.get('objectName'), width=width)
  print '{0:{width}} {1}'.format('Object type:', json_.get('objectType'), width=width)
  print '{0:{width}} {1}'.format('Object ID:', json_.get('objectID'), width=width)
  print '{0:{width}} {1}'.format('Parent URI:', json_.get('parentURI'), width=width)
  print '{0:{width}} {1}'.format('Parent ID:', json_.get('parentID'), width=width)

  if object_type == "application/cdmi-container":
    print '{0:{width}} {1}'.format('Capabilities URI:', json_.get('capabilitiesURI'), width=width)
    print '{0:{width}} {1}'.format('Domain URI:', json_.get('domainURI'), width=width)
    print '{0:{width}} {1}'.format('Completion status:', json_.get('completionStatus'), width=width)
    if json_.has_key('children'):
      print '{0:{width}}'.format('Children:', width=width)
      for child in json_.get('children', []):
        print '{0:{width}} {1}'.format(' ', child, width=width)
    if json_.has_key('exports'):
      print '{0:{width}}'.format('Supported data protocols:', width=width)
      for protocol in json_.get('exports', dict()):
        print '{0:{indent}}{1}'.format(' ', protocol + ':', indent=indent)
        protocol_dict = json_.get('exports', dict()).get(protocol)
        for key in protocol_dict:
          print '{0:{indent}}{1:{indent}}{2:{width}} {3}'.format(' ', ' ', key + ':', protocol_dict[key], indent=indent, width=double_indent_width)

  elif object_type == "application/cdmi-object":
    print '{0:{width}} {1}'.format('Capabilities URI:', json_.get('capabilitiesURI'), width=width)
    print '{0:{width}} {1}'.format('Domain URI:', json_.get('domainURI'), width=width)
    print '{0:{width}} {1}'.format('Completion status:', json_.get('completionStatus'), width=width)
    print '{0:{width}} {1}'.format('MIME type:', json_.get('mimetype'), width=width)

  elif object_type == "application/cdmi-capability":
    if json_.has_key('children'):
      print '{0:{width}}'.format('Children:', width=width)
      for child in json_.get('children', []):
        print '{0:{width}} {1}'.format(' ', child, width=width)
    if json_.has_key('metadata') and json_.get('metadata'):
      print '{0:{width}}'.format('Capabilities:', width=width)
      print '{0:{indent}}{1:{width}} {2}'.format(' ', 'Data redundancy:', json_.get('metadata', dict()).get('cdmi_data_redundancy'), indent=indent, width=indent_width)
      print '{0:{indent}}{1:{width}} {2}'.format(' ', 'Latency (ms):', json_.get('metadata', dict()).get('cdmi_latency'), indent=indent, width=indent_width)
      print '{0:{indent}}{1:{width}} {2}'.format(' ', 'Throughput (bps):', json_.get('metadata', dict()).get('cdmi_throughput'), indent=indent, width=indent_width)
      print '{0:{indent}}{1:{width}} {2}'.format(' ', 'Capabilities allowed:', json_.get('metadata', dict()).get('cdmi_capabilities_allowed'), indent=indent, width=indent_width)
      print '{0:{indent}}{1:{width}} {2}'.format(' ', 'Geographic placement:', json_.get('metadata', dict()).get('cdmi_geographic_placement'), indent=indent, width=indent_width)

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
  import requests
  from requests.packages.urllib3.exceptions import InsecureRequestWarning
  requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

  if not host:
    print "!error: you need to open a connection to a CDMI server first"
    return

  path = path.strip().lstrip('/')
  capabilities_uri = capabilities.strip().lstrip('/')

  headers = {'X-CDMI-Specification-Version': '1.1.1'}

  if capabilities_uri.find('cdmi_capabilities/dataobject') != -1:
    headers['Content-Type'] = 'application/cdmi-object'
  elif capabilities_uri.find('cdmi_capabilities/container') != -1:
    headers['Content-Type'] = 'application/cdmi-container'
  else:
    print "!error: wrong capabilities URI format"
    return

  data = {'capabilitiesURI': '/{0}'.format(capabilities_uri)}
  r = None
  try:
    if user:
      r = requests.put('https://{0}:{1}/{2}'.format(host, port, path), verify=False, auth=(user,password), headers=headers, data=json.dumps(data))
    elif token:
      headers['Authentication'] = 'Bearer {0}'.format(token)
      r = requests.put('https://{0}:{1}/{2}'.format(host, port, path), verify=False, headers=headers, data=json.dumps(data))
    else:
      print "!error: you need to provide authentication first"
      return
  except requests.exceptions.ConnectionError:
    try:
      if user:
        r = requests.put('http://{0}:{1}/{2}'.format(host, port, path), auth=(user,password), headers=headers, data=json.dumps(data))
      elif token:
        headers['Authentication'] = 'Bearer {0}'.format(token)
        r = requests.put('http://{0}:{1}/{2}'.format(host, port, path), verify=False, headers=headers, data=json.dumps(data))
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
  elif r.status_code == 204:
    print "... QoS transition committed ..."

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
