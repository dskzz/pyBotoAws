import boto3
import botocore
import mechanize
from pprint import pprint
import datetime
import os 
from tempfile  import mkstemp
from skzlogger2 import skzz_log_control2


# : Version Number - Bucket Handler
VERSION_NUMBER = '0.1'

class bucket_handle( object ):
      s3_client = None
      practice_mode = None          # This shows the result, but doesn't take actions
      aws_bucket_name = None
      aws_dir = None
      aws_file_name = None
      bucket_list = None
      last_error = None

      #the_log = skz_logger( )
      the_log = skzz_log_control2( )

      dl_dir = None
      pp = None

      local_path = None
      turn_on_boto_debugging = None

      def __init__(self, aws_key = None, aws_secret = None, region = None, debug = None):
            self.aws_key = aws_key
            self.aws_secret = aws_secret
            self.aws_region = region

            # Just in case we need it, set up a browser.
            self.br = mechanize.Browser()
            self.br.set_handle_robots(False)
            
            # And the S3 client 
            self.s3_client_connect( )
           
            self.last_err = ''
            self.dl_dir = '.\\'
            self.the_log.create_default()

            self.the_log.set_log_attr_all_logs( log_prompt = 'B')

            if debug == True:
                  self.turn_on_boto_debugging = True
            #self.the_log.dump_logs( )

      def toggle_debug( self, override = None ):
            if override is None:
                  if self.turn_on_boto_debugging is True:
                        self.turn_on_boto_debugging = False
                  else:
                        self.turn_on_boto_debugging = True
            else:
                  self.turn_on_boto_debugging = override
            
            if self.turn_on_boto_debugging is True:
                  boto3.set_stream_logger('botocore')
                  boto3.set_stream_logger('boto3')

      def dump_logs( self ):
            self.the_log.dump_logs( )

      def set_local_path( path ):
            self.local_path = path

            #pp = pprint.PrettyPrinter(indent=4)     
      def setup_logging( self, log_loc ):
            pass
            # self.the_log.set_write_location( log_loc )
            
      def set_log_level( self, screen_level, file_level = None ):
            if file_level is None:
                  file_level = screen_level

            self.the_log.set_log_attr( 'file', log_level = file_level )
            self.the_log.set_log_attr( 'screen', log_level = screen_level )

      def get_current( self ):
            ret =  "Bucket: " + self.aws_bucket_name
            ret +=  "Path: " + self.aws_dir
            ret += "File: " + self.aws_file_name
            return ret

      def print_current( self ):
            print ("Bucket: " + self.aws_bucket_name)
            print ("Path: " + self.aws_dir)
            print ("File: " + self.aws_file_name)

      def set_dl_dir( self, dir ):
            if dir is not None:
                  self.dl_dir = self.ensure_dir_ends_with_slash( dir )

      # For resource, not client... resource(service_name, region_name=None, api_version=None, use_ssl=True, verify=None, endpoint_url=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, config=None)
      def s3_client_connect( self ):
            res = ''
            
            if self.aws_key is not None and self.aws_secret is not None:
                  self.s3_client = boto3.client('s3', self.aws_region, None, True, None, None, self.aws_key, self.aws_secret )
            else:
                 self.s3_client = boto3.client('s3', self.aws_region )  # try to load from system defaults.\
            
            
            try:
                  bucket_list = self.s3_client.list_buckets( )
            except Exception as e:
                  self.error_handler( e )
                  return -1
            return 1


      def error_handler( self, exception ):
            error = None
            print (exception)
            if str(exception).find( "does not match the signature you provided" ):
                  error = "Bad Secret Key"
            elif exception.find( "Access Key Id you provided does not exist in our records" ):
                  error = "Bad Access Key: "+ self.aws_key
                  
      def ensure_dir_ends_with_net_slash( self, dir ):
            if dir is None or dir == "":
                  return ""

            if dir[-1] == '\\':
                  dir = dir[:-1] +"/"
                  return dir
            
            if dir[-1] == '/':
                  return dir 

            if dir[-1] != '/' and dir[-1] != "\\":
                  return dir + "/"

            return dir

      def ensure_dir_ends_with_slash( self, dir ):
            if dir[-1] != '\\':
                  dir += "\\"
            return dir

      def set_region( self, region=None ):
            if region:
                  self.region = region

      # send either key,secret or key:secret or just key
      def new_key_pair( self, key = None, secret = None ):
            if key is None:
                  log("Need to supply creds.")
                  return 0
            if secret is None and key is not None:
                  if key.find(":"):
                        self.aws_key, self.aws_secret = key.split(":")
                  else:
                          self.aws_key = key
            
            elif secret is not None and key is not None:
                  self.aws_key = key
                  self.aws_secret = secret
            else:
                  log("Trying to log in without providing credentials.  Should default to the creds in user's personal .aws folder. ")

            return self.s3_client_connect( )
      #OK the main writer function - print to both screen and the log
      def log( self, txt, lvl = None, prompt_once = None, disable_screen = None, disable_write = None ):
            if txt is None:
                  return
            self.the_log.log(txt, lvl, 'B' )

      # Getting s3://bucket/dir/file
      def parse_address( self,  addr ):
            print ('x')

      def practice_mode_on_off( self, turn_on ):
            if turn_on == 1:
                  self.practice_mode = 1
            else:
                 self.practice_mode = None 

      def download_by_file( self,  bucket_name, directory, file_name, dl_dir = None ):
            self.mode = 'BUCKET_DIRECT'
            me = file_name
            self.aws_dir = directory
            self.aws_bucket_name = bucket_name
            self.aws_file_name = file_name

            self.log("download_by_file: Bucket: " + bucket_name + " Directory: " + directory + " File name: " + file_name + " DL dir: " + dl_dir, 3 )
            if self.aws_dir != "" and self.aws_dir is not None:
                 # if first part is slashed, remove
                  if   self.aws_dir[0] == "/":
                        self.aws_dir =   self.aws_dir[1:] 

                  if file_name[0] == "/":
                        self.file_name = file_name[1:] 

                  # if end is not slashed, slash
                  if   self.aws_dir[-1] != "/":
                        self.aws_dir +=  "/"

                  if self.aws_dir == '/':
                        self.aws_dir = ''

            # clean start and end slashes for bucket.
            if bucket_name[-1] == "/":
                  self.aws_bucket_name = bucket_name[:-1] 

            if bucket_name[0] == "/":
                  self.aws_bucket_name = bucket_name[1:] 


            if len( self.aws_bucket_name ) < 1 :
                  self.log ( "Failed because bucket name didn't register; uri: " + uri )
                  self.last_err = 'BUCKETNAME_FAILED'
                  return -1

            if len( self.aws_file_name ) < 1 :
                  self.log ( "Failed because filename name didn't register; uri: " + uri )
                  self.last_err = 'FILENAME_FAILED'
                  return -1

            return self._get_bucket_file( dl_dir )


     #includes using this system's s3:// notation
      def get_http( self, uri, return_mode = None ):
            self.target_uri = uri
            if uri.startswith( 'http://'):
                  self.mode = 'HTTP_GET'
                  return self._download_non_bucket( uri, tgt_dir, return_mode )


      #includes using this system's s3:// notation
      def download_by_uri( self, uri, tgt_dir = None, tgt_file = None ):
            self.target_uri = uri
            if tgt_dir is None:
                  self.log( "download_by_uri: " + str(uri) + " to dl_dir = " + self.dl_dir , 3  )
            else:
                 self.log( "download_by_uri: " + str(uri) + " to tgt_dir = " + str(tgt_dir) , 3  )
                  
            if uri.startswith( 's3://'):
                 self._break_bucket_uri_to_parts( uri )
                 self.mode = 'BUCKET_URI'
                 return self._get_bucket_file( tgt_dir, tgt_file )

            elif uri.startswith( 'http://'):
                  self.mode = 'HTTP_GET'
                  return self._download_non_bucket( uri, tgt_dir, tgt_file )


      # this is mainly in there to get JSON from remote location.
      def _download_non_bucket( self,  uri,      tgt_dir = None, tgt_file = None,      return_mode = None, binmode = None ):
            dl_dir = self._pick_dir( tgt_dir )
            
            if tgt_file is None:
                  file_name = self._get_filename_from_uri( uri )
            else:
                  file_name = tgt_file

            
            self.log("_download_non_bucket: uri: " + str(uri) + " tgt_dir: " + str(tgt_dir) + " Write mode: " + str(return_mode) + " binmode: " + str(binmode), 3)
            
            ret = None
            response = self.br.open( uri )
            if( response.code > 299 or response.code < 200 ):
                  self.log("ERROR: Reponse code for getting " + uri + "  Code: "+ str(response.code), 4 )
                  self.last_err = response.code
                  return -1
            else:
                  self.log("GET " + uri + "  Success.  Code: "+ str(response.code), 4 )
                  ret = response.read()

            if tgt_dir is not None:
                  tgt_file = self.ensure_dir_ends_with_slash( tgt_dir ) + file_name
                  self.write_file( ret, tgt_file, binmode )

            if return_mode is not None:
                  return ret
            else:  # TODO - should have some error handling here; ret a neg 1 maybe...
                  return self.write_file( ret, dl_dir + file_name, binmode )

      def write_file( self, txt, file_name, binmode = None ):
            if( binmode == None ):
                  try:
                        self.log("Saving file to "+file_name, 3)
                        text_file = open( file_name, 'w' )
                        text_file.write( txt )
                        text_file.close()
                  except:
                        log( "Failed to open and write to text file " + file_name, 4 )
                        return -1
            else:
                  try:
                        cur = 0
                        bin_file = open( file_name, 'ab' )
                        
                        while cur < len(txt):
                              c = int(txt[cur:cur+8], 2)
                              bin_file.write(bytes(chr(c), 'iso8859-1'))
                              cur += 8
                        bin_file.close()

                  except:
                        log( "Failed to open and write to binary file " + file_name, 4 )
                        return -1
            return file_name

      def _get_filename_from_uri( self, uri ):
            if uri is None or uri == "":
                  return ""
            path =  uri.split( '/')
            file = path.pop()

            # for params in uri
            q_mark_loc = file.find("?")
            if q_mark_loc > -1:
                  file_with_args = file.split("\?")
                  file = file[:q_mark_loc]

            if file == "":
                  return ''

            return file

      def get_path_from_path( self, path ):
            try:
                  head, tail = os.path.split( path )
                  return self.ensure_dir_ends_with_slash( head )
            except:
                  return None

      def get_filename_from_path( self, path ):
            parts = path.split( "\\" )
            return parts.pop( )


      def _break_bucket_uri_to_parts( self,  uri ):
            if not uri.startswith( 's3://'):
                  return -1

            bucket_simplified =  uri.split( 's3://', 1)[1]
            path =  uri.split( 's3://')[1]

            pieces = path.split("/")
            length = len(pieces)

            self.aws_bucket_name = pieces[0]
            self.aws_file_name = pieces.pop()
                       
            self.aws_dir = "/".join( pieces[1:] ) + "/"

            if self.aws_dir == "/":
                  self.aws_dir = ""

            if len( self.aws_bucket_name ) < 1 :
                  self.log ( "Failed because bucket name didn't register; uri: " + uri, 4 )
                  self.last_err = 'BUCKETNAME_FAILED'
                  return -1
      
            #if len( self.aws_file_name ) < 1 :
            #      self.log ( "Failed because filename name didn't register; uri: " + uri )
            #      self.last_err = 'FILENAME_FAILED'
            #      return -1
            
            return 1

      

      def _pick_dir( self, tmp_dir = None ):
            return tmp_dir
            if tmp_dir is not None:
                  return self.prep_uri_path( tmp_dir  )
            elif self.dl_dir is not None and tmp_dir is None:
                  return self.dl_dir
            else:
                  self.err("Warning - you should really specify a directory")
                  return ".\\"

      def _get_bucket_file( self, tmp_dir = None, tmp_file = None  ):
            s3 = self.get_s3( '_get_bucket_file' )
            dl_file = self.aws_dir + self.aws_file_name
            
            if tmp_file is None:
                  out_file = self._pick_dir( tmp_dir ) + self.aws_file_name
            else:
                 out_file = self._pick_dir( tmp_dir ) + tmp_file 

            self.log("Download from: "+ str(dl_file), 3)
            self.log("Save to: "+ str(out_file), 3 )
            out_file_original = out_file
            #self.print_current()

            # print "Downloading " , self.aws_bucket_name ,"/", dl_file, " to ", out_file 
            if self.practice_mode == 1:
            #      print "Practice mode is on.  Not actually downloading. "
                  return out_file

            try:
                  self.log("Running  s3.download_file( "+self.aws_bucket_name+", "+dl_file+", " +out_file +")", 3)
                  s3.download_file( str(self.aws_bucket_name), str(dl_file ), str(out_file) )

            except botocore.exceptions.ClientError as e:
                  self.log("Exception: " + str(e) + " in Get_bucket_file: Failed to DL " + dl_file + " from " + self.aws_bucket_name + ' ' +dl_file , 5)
                  print " "
                  self.last_error = e
                  return -1   
            return out_file_original

      """
            Get the error, if ret = True, then return the variable, else pretty print it.
      """
      def get_error( self, ret = False):

            if ret is False:
                  print "\nBOTO LAST ERROR:"
                  pprint( self.last_error )
                  print "\n###\n\n\n"
            else:
                  return self.last_error


      def get_s3 ( self, caller = None ):
            err = 'S3 FAILED TO INITIALIZE'
            if caller is not None: 
                  err = caller + ": " + err
            
            if self.s3_client is None or self.s3_client == "":
                  log( err , 4 )
                  return -2

            return self.s3_client

      def check_file_exists( self, file ):
            if( os.path.isfile( file ) ):
                  return True
            else:
                  return False

      def check_dir( self, file_path):
            directory = os.path.dirname(file_path)
            if not os.path.exists(directory):
                  return True
            else:
                  return False
      
      def ensure_dir(self, file_path):
            directory = os.path.dirname(file_path)
            if not os.path.exists(directory):
                  try:
                        os.makedirs(directory)
                  except:
                        return False

      def prep_uri_path( self, path = None ):
            if path is None or path == "":
                  return ""
            else:
                  if path[0] == "/":
                        path = path[1:]
                  if path[-1] != "/":
                        path = path + "/"
            return path


      # s3://bname/dir/  NO FILE! Blank reuses
      def put_by_uri( self, file, uri = None, tgt_dir = None ):           
            s3 = self.get_s3( 'PUT by URI')
            self.log("Put_by_uri: Attempting to upload [" + str( file ) + "] to [" + str( uri ) +"] with dir [" + str(tgt_dir) +"]", 3)
            
            if uri is not None:
                  if self.check_if_uri_has_file( uri ) is not None:
                        tgt_dir = None          #override because using PUT type of mode
                  else:
                        uri = self.prep_uri_path( uri )     #otherwise clean our uri

                  if tgt_dir is not None and tgt_dir != '':
                        tgt_dir = self.prep_uri_path( tgt_dir )
                        uri = uri + tgt_dir

                  self._break_bucket_uri_to_parts( uri )
                  if self.aws_file_name is None or self.aws_file_name == "":
                        self.aws_file_name = self.get_filename_from_path( file )
            else:
                  if self.aws_bucket_name == None or  self.aws_bucket_name == "":
                         self.log( "put_by_uri: PUT Failed. No URI provided and BUCKET NAME is NULL" , 4 )
            
            if self.aws_file_name == None or self.aws_file_name == "":
                  self.log( "put_by_uri: File failed. Failed to PUT [" + str( file ) + "] to " + str( uri ) , 4 )
                  return -1

            if self.check_file_exists( file ) == False:
                  self.log( "put_by_uri: File does not exist! Could not PUT [" + file + "]" , 4 )
                  return -1
            else:
                  self.log( "put_by_uri: File found. Proceeding to PUT [" + file + "]" , 4 )
                
            upload_path = self.aws_dir + self.aws_file_name
            if self.practice_mode == 1:
                  print ("s3.upload_file( " + file +", " + self.aws_bucket_name  +", "+  upload_path +" )")
            else:
                  try:
                        s3.upload_file( file, self.aws_bucket_name,  upload_path )
                  except Exception as e:
                        err_msg = "Put by File: Failed to upload: " + file_loc + "\n" + str( e ) +"\n"+self.get_current( )
                        self.log( err_msg, 4 )
                        return -1
            return 1

      def try_with_s3_notation( self ):
            s3 = self.get_s3( 'TEST' )
            file = 's3://atmtest/cadi/t1.txt'
            dest = "C:\\Users\\Web\\Documents\\_DEV\\test.txt"
            s3.download_file( file, dest )

      def clean_bucket_no_slashes( self, bucket_name ):
            if bucket_name[-1] == "/":
                  bucket_name = bucket_name[:-1] 

            if bucket_name[0] == "/":
                  bucket_name = bucket_name[1:] 

            return bucket_name

      def put_by_file( self, file_loc, bucket_name = None, bkt_path = None ):           
            new_path = None
            if bucket_name.find("/") != -1:
                  bucket_name, new_prath = bucket_name.split('/', 1)

            bucket_name = self.clean_bucket_no_slashes( bucket_name )
            if new_path is not None and bkt_path is not None:
                  bkt_path = self.prep_uri_path( new_path ) + self.prep_uri_path( bkt_path )

            if( self.check_file_exists( file_loc ) == False ):
                  self.log("put_by_file: Failed to put by file because [" + file_loc + "] does not exist.", 4)

            if bucket_name == None and ( self.aws_bucket_name == None or self.aws_bucket_name == "" ):
                  self.log("put_by_file: Failed to put by file because no bucket specified and no bucket is saved.", 4)
                  return -1 

            if bkt_path is None:
                  bkt_path = ''

            file_name = self.get_filename_from_path( file_loc )
            self.aws_bucket_name = bucket_name
            self.aws_file_name = file_name
            self.aws_dir = bkt_path
           
            if len( self.aws_bucket_name ) < 1 :
                  self.log ( "Put by File: Failed because bucket name didn't register; uri: " + uri )
                  self.last_err = 'BUCKETNAME_FAILED'
                  return -1

            if len( self.aws_file_name ) < 1 :
                  self.log ( "put_by_file: Failed because filename name didn't register; uri: " + uri )
                  self.last_err = 'FILENAME_FAILED'
                  return -1

            upload_path = self.aws_dir + self.aws_file_name
            
            self.log("put_by_file:  Attempting to upload [" + str( file_loc ) + "] to [" + str( bucket_name ) +"] with dir [" + str(bkt_path)+"]", 3)

            s3 = self.get_s3( 'PUT by FILE')
            try:
                  s3.upload_file( file_loc, self.aws_bucket_name,  upload_path )
            except Exception as e:
                  err_msg = "Put by File: Failed to upload: " + file_loc + "\n" + str( e ) +"\n"+self.get_current( )
                  self.log( err_msg, 4 )
                  return -1
            return 1


      def list_bucket_content( self, bucket, dir ):
            pass

      def list_bucket_content_uri( self, bucket, dir ):
            pass

      def using_http( self, uri ):
            if uri.startswith('http://'):
                  return True
            else:
                  return False

      def using_bucket_notation( self, uri ):
            if uri.startswith('s3://'):
                  return True
            else:
                  return False

      def check_if_uri_has_file( self, uri ):
            if uri.endswith('/'):
                  return None

            last = uri.split('/')[-1]
            if last == '' or last is None:
                  return None
            else:
                  if last.find("."):
                        return last

      def clarify_dl_location ( self, path = None, file = None ):
            if file is None:
                  if path is not None:
                        if path[-1] == '/' or path[-1] == "\\":
                              return (path, None ) 
                        else:
                              head, tail = os.path.split( path )
                              head = self.ensure_dir_ends_with_slash( head )
                              self.ensure_dir( head )
                              return ( head, tail )
                  else:
                        if self.dl_dir is not None:
                              return ( self.dl_dir, None )
                        else:
                              self.err( "Warning, no directory is set for downloading." )
                              return ( None, None )
            else:
                  path = self.ensure_dir_ends_with_slash( path )
                  self.ensure_dir( path )
                  return ( path, file )


      # PUT - use same bucket if none.  
      def PUT( self, file, uri_or_bucket = None, tgt_dir = None ):
            if uri_or_bucket is not None:
                  if self.using_bucket_notation( uri_or_bucket ) == True:
                        return self.put_by_uri( file, uri_or_bucket, tgt_dir )
                  else:
                        return self.put_by_file( file, uri_or_bucket, tgt_dir )       
            else:
                  return self.put_by_file( file, uri_or_bucket, tgt_dir )


      # PUT - use same bucket if none.  
      def PUT_TXT( self, txt, uri_or_bucket = None, tgt_dir = None ):
            fd, file_name = mkstemp( )
#            f = os.fdopen( fd )
            os.write(fd, txt)
            self.PUT( file_name, uri_or_bucket, tgt_dir )
            os.close(fd)
            os.remove(file_name)

      """
            GET - feed it html, s3 or specify names of bucket, dir, file.  
            as for downloading, ending with a \ will assume dir and set file name to the name of file downloaded.
            Ending with text will assume tail of path is filename.

            GET is polymorphized:
                  GET( s3://bucket/wherever )
                  GET( s3://bucket/wherever, target_loc )
                  GET( s3://bucket/wherever, target_dir, file_name )
                  GET( bucket_name, bucket_path, bucket_file, target_save_dir )
                  GET( http://whatever )  ... and same as s3:// options.
      """
      def GET_PATH( self, uri_or_bucket, path1 = None , file  = None, path2 = None ):
            self.log("Simulating GET for path: "+ uri_or_bucket,3)
            
            out_file = None
            if self.using_bucket_notation( uri_or_bucket ) == True:
                  self.log("Using s3:// notation",2 )
                  
                  (this_path, this_file) = self.clarify_dl_location( path1, file )
                  if this_file is None:
                        out_file =  self.ensure_dir_ends_with_slash( self._pick_dir( this_path ) ) + self._get_filename_from_uri( uri_or_bucket )
                  else:
                        out_file = self.ensure_dir_ends_with_slash( self._pick_dir( this_path ) ) + this_file 

            elif self.using_http( uri_or_bucket ) == True:
                  (this_path, this_file) = self.clarify_dl_location( path1, file )
                  if this_file is None:
                        out_file =  self.ensure_dir_ends_with_slash( self._pick_dir( this_path ) ) + self._get_filename_from_uri( uri_or_bucket )
                  else:
                        out_file = self.ensure_dir_ends_with_slash( self._pick_dir( this_path ) ) + this_file 

            else:
                  if( file is not None ):
                        self.log("Using the file download method",2)
                        (this_path, this_file) = self.clarify_dl_location( path2, file )
                        if this_file is None:
                              out_file =  self.ensure_dir_ends_with_slash( self._pick_dir( this_path ) ) + self._get_filename_from_uri( uri_or_bucket )
                        else:
                              out_file = self.ensure_dir_ends_with_slash( self._pick_dir( this_path ) ) + this_file 
                  else:
                        self.log("Not enough information to simulate download a file. URI/bucket: "+ str(uri_or_bucket ) + "; path: "+ path  + "; file: "+ file  + "; tgt_dir: " + tgt_dir )
                        return -1
            return out_file


      def GET( self, uri_or_bucket, path1 = None , file  = None, path2 = None):            
            self.log("Running GET "+ uri_or_bucket,3)

            if self.using_bucket_notation( uri_or_bucket ) == True:
                  self.log("Using s3:// notation",2 )
                  (this_path, this_file) = self.clarify_dl_location( path1, file )

                  return self.download_by_uri( uri_or_bucket, this_path, this_file )

            elif self.using_http( uri_or_bucket ) == True:
                  self.log("Getting via HTML",2)
                  (this_path, this_file) = self.clarify_dl_location( path1, file )
                  return self._download_non_bucket( uri_or_bucket, this_path, this_file )

            else:
                  if( file is not None ):
                        self.log("Using the file download method",2)
                        return self.download_by_file( uri_or_bucket, path1, file, path2 )
                  else:
                        self.log("Not enough information to download a file. URI/bucket: "+ str(uri_or_bucket ) + "; path: "+ path  + "; file: "+ file  + "; tgt_dir: " + tgt_dir )
                        return -1
            return 1

      def GET_TXT( self, uri_or_bucket ):
            self.log("Running GET_txt: "+ uri_or_bucket,3)            
            fd, path = mkstemp( )
            os.close(fd)
#            f = os.fdopen( fd )
            #print uri_or_bucket
            #print path
            #path = 'C:\Users\Web\Desktop\cadillac-manifest-downloader\dist\\'
            head, tail = os.path.split( path )
            #print head
            #print tail            
            self.GET( uri_or_bucket, head, tail )
            
            f = open( path, "r" )
            rval = f.read()
            f.close()
            return rval
            try:
                  os.remove( path )
            except:
                  self.log("Counldn't remove "+path, 2)

            if self.using_bucket_notation( uri_or_bucket ) == True:
                  self.log("Using s3:// notation",2 )
                  return self.download_by_uri( uri_or_bucket, path1 )

            elif self.using_http( uri_or_bucket ) == True:
                  self.log("Getting via HTML",2)
                  return self._download_non_bucket( uri_or_bucket, path1 )
            else:
                  if( file is not None ):
                        self.log("Using the file download method",2)
                        return self.download_by_file( uri_or_bucket, path1, file, path2 )
                  else:
                        self.log("Not enough information to download a file. URI/bucket: "+ str(uri_or_bucket ) + "; path: "+ path  + "; file: "+ file  + "; tgt_dir: " + tgt_dir )
                        return -1
            return 1




### END ###






